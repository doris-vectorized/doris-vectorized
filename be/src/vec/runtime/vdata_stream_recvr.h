#pragma once

#include <atomic>
#include <condition_variable>
#include <list>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/query_statistics.h"
#include "util/runtime_profile.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {
class MemTracker;
class RuntimeProfile;
namespace vectorized {
class Block;
class VDataStreamMgr;
class VDataStreamRecvr {
public:
    VDataStreamRecvr(VDataStreamMgr* stream_mgr, const std::shared_ptr<MemTracker>& parent_tracker,
                     const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
                     PlanNodeId dest_node_id, int num_senders, bool is_merging,
                     int total_buffer_limit, RuntimeProfile* profile,
                     std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    ~VDataStreamRecvr();
    void add_batch(Block* block, int sender_id, bool use_move);
    // TODO: merger

    Status get_next(Block* block, bool* eos);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }
    std::shared_ptr<MemTracker> mem_tracker() const { return _mem_tracker; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
    }

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number);

private:
    class SenderQueue;

    void cancel_stream();
    bool exceeds_limit(int batch_size) {
        return _num_buffered_bytes + batch_size > _total_buffer_limit;
    }

    // TODO:
    // DataStreamMgr instance used to create this recvr. (Not owned)
    VDataStreamMgr* _mgr;

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // soft upper limit on the total amount of buffering allowed for this stream across
    // all sender queues. we stop acking incoming data once the amount of buffered data
    // exceeds this value
    int _total_buffer_limit;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;

    std::atomic<int> _num_buffered_bytes;
    std::shared_ptr<MemTracker> _mem_tracker;
    std::vector<SenderQueue*> _sender_queues;

    // TODO: declare merger
    ObjectPool _sender_queue_pool;
    RuntimeProfile* _profile;

    RuntimeProfile::Counter* _bytes_received_counter;
    RuntimeProfile::Counter* _deserialize_row_batch_timer;
    RuntimeProfile::Counter* _first_batch_wait_total_timer;
    RuntimeProfile::Counter* _buffer_full_total_timer;
    RuntimeProfile::Counter* _data_arrival_timer;

    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;
};

class VDataStreamRecvr::SenderQueue {
public:
    SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile);

    ~SenderQueue() {}

    Status get_batch(Block** next_block);

    void decrement_senders(int sender_id);

    void cancel();

    void close();

    Block* current_block() const { return _current_block.get(); }

private:
    VDataStreamRecvr* _recvr;
    std::mutex _lock;
    bool _is_cancelled;
    int _num_remaining_senders;
    std::condition_variable _data_arrival_cv;
    std::condition_variable _data_removal_cv;

    using VecBlockQueue = std::list<std::pair<int, Block*>>;
    VecBlockQueue _block_queue;

    std::unique_ptr<Block> _current_block;

    bool _received_first_batch;
    // sender_id
    std::unordered_set<int> _sender_eos_set;
    // be_number => packet_seq
    std::unordered_map<int, int64_t> _packet_seq_map;
    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;
};
} // namespace vectorized
} // namespace doris
