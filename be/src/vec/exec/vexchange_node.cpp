#include "vec/exec/vexchange_node.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::vectorized {
VExchangeNode::VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _stream_recvr(nullptr) {}

Status VExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }
    DCHECK(!_is_merging) << "now we don't support merge";
    // TODO: sort code
    return Status::OK();
}

Status VExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _convert_row_batch_timer = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile.get(), _is_merging,
            _sub_plan_query_statistics_recvr);

    DCHECK(!_is_merging);
    // TODO: sort code
    return Status::OK();
}
Status VExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    // TODO: sort
    return Status::OK();
}
Status VExchangeNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VExchange Node::get_next scalar");
}
Status VExchangeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _stream_recvr->get_next(block, eos);
}
Status VExchangeNode::close(RuntimeState* state) {
    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    return ExecNode::close(state);
}

} // namespace doris::vectorized
