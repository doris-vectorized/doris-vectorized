#include "vec/runtime/vdata_stream_recvr.h"

#include "runtime/mem_tracker.h"
#include "util/uid_util.h"
#include "vec/core/block.h"

namespace doris::vectorized {

VDataStreamRecvr::SenderQueue::SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders,
                                           RuntimeProfile* profile)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _received_first_batch(false) {}

Status VDataStreamRecvr::SenderQueue::get_batch(Block** next_block) {
    std::unique_lock<std::mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0) {
        VLOG_ROW << "wait arrival fragment_instance_id=" << _recvr->fragment_instance_id()
                 << " node=" << _recvr->dest_node_id();
        // Don't count time spent waiting on the sender as active time.
        CANCEL_SAFE_SCOPED_TIMER(_recvr->_data_arrival_timer, &_is_cancelled);
        CANCEL_SAFE_SCOPED_TIMER(
                _received_first_batch ? NULL : _recvr->_first_batch_wait_total_timer,
                &_is_cancelled);
        _data_arrival_cv.wait(l);
    }

    // _cur_batch must be replaced with the returned batch.
    _current_block.reset();
    *next_block = nullptr;
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    if (_block_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return Status::OK();
    }

    _received_first_batch = true;

    DCHECK(!_block_queue.empty());
    Block* result = _block_queue.front().second;
    _recvr->_num_buffered_bytes -= _block_queue.front().first;
    VLOG_ROW << "fetched #rows=" << result->rows();
    _block_queue.pop_front();

    _current_block.reset(result);
    *next_block = _current_block.get();

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }

    return Status::OK();
}

VDataStreamRecvr::VDataStreamRecvr(
        VDataStreamMgr* stream_mgr, const std::shared_ptr<MemTracker>& parent_tracker,
        const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, bool is_merging, int total_buffer_limit,
        RuntimeProfile* profile,
        std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _num_buffered_bytes(0),
          _profile(profile),
          _sub_plan_query_statistics_recvr(sub_plan_query_statistics_recvr) {
    _mem_tracker = MemTracker::CreateTracker(
            _profile, -1, "VDataStreamRecvr:" + print_id(_fragment_instance_id), parent_tracker);

    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue =
                _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue, profile));
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _bytes_received_counter = ADD_COUNTER(_profile, "BytesReceived", TUnit::BYTES);

    _deserialize_row_batch_timer = ADD_TIMER(_profile, "DeserializeRowBatchTimer");
    _data_arrival_timer = ADD_TIMER(_profile, "DataArrivalWaitTime");
    _buffer_full_total_timer = ADD_TIMER(_profile, "SendersBlockedTotalTimer(*)");
    _first_batch_wait_total_timer = ADD_TIMER(_profile, "FirstBatchArrivalWaitTime");
}

VDataStreamRecvr::~VDataStreamRecvr() {
    DCHECK(_mgr == NULL) << "Must call close()";
}

} // namespace doris::vectorized