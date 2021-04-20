#pragma once
#include "common/global_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/network_util.h"
#include "util/ref_count_closure.h"
#include "util/uid_util.h"
#include "vec/exprs/vexpr.h"
#include "vec/sink/data_sink.h"

namespace doris {
class ObjectPool;
class RowBatch;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class MemTracker;
class PartitionInfo;

namespace vectorized {
class VExprContext;

class VDataSender : public VDataSink {
public:
    VDataSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                const TDataStreamSink& sink,
                const std::vector<TPlanFragmentDestination>& destinations,
                int per_channel_buffer_size, bool send_query_statistics_with_every_batch);

    ~VDataSender();

    virtual Status init(const TDataSink& thrift_sink) override;

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;

    virtual Status send(RuntimeState* state, RowBatch* batch) override;
    virtual Status send(RuntimeState* state, Block* block) override;

    virtual Status close(RuntimeState* state, Status exec_status) override;
    virtual RuntimeProfile* profile() override { return _profile; }

    RuntimeState* state() { return _state; }

private:
    class Channel;

    /* Status compute_range_part_code(RuntimeState* state, TupleRow* row, size_t* hash_value,*/
    /*bool* ignore);*/

    //int binary_find_partition(const PartRangeKey& key) const;

    //Status find_partition(RuntimeState* state, TupleRow* row, PartitionInfo** info, bool* ignore);

    //Status process_distribute(RuntimeState* state, TupleRow* row, const PartitionInfo* part,
    //size_t* hash_val);

    // Sender instance id, unique within a fragment.
    int _sender_id;

    RuntimeState* _state;
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;

    int _current_channel_idx; // index of current channel to send to if _random == true

    TPartitionType::type _part_type;
    bool _ignore_not_found;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PRowBatch _pb_batch1;
    PRowBatch _pb_batch2;
    PRowBatch* _current_pb_batch = nullptr;

    // compute per-row partition values
    std::vector<VExprContext*> _partition_expr_ctxs;

    std::vector<Channel*> _channels;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<PartitionInfo*> _partition_infos;

    RuntimeProfile* _profile; // Allocated from _pool
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeProfile::Counter* _ignore_rows;

    std::shared_ptr<MemTracker> _mem_tracker;

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput;

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;
};

// TODO: support local exechange

class VDataSender::Channel {
public:
    // Create channel to send data to particular ipaddress/port/query/node
    // combination. buffer_size is specified in bytes and a soft limit on
    // how much tuple data is getting accumulated before being sent; it only applies
    // when data is added via add_row() and not sent directly via send_batch().
    Channel(VDataSender* parent, const RowDescriptor& row_desc, const TNetworkAddress& brpc_dest,
            const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int buffer_size,
            bool is_transfer_chain, bool send_query_statistics_with_every_batch)
            : _parent(parent),
              _buffer_size(buffer_size),
              _row_desc(row_desc),
              _fragment_instance_id(fragment_instance_id),
              _dest_node_id(dest_node_id),
              _num_data_bytes_sent(0),
              _packet_seq(0),
              _need_close(false),
              _brpc_dest_addr(brpc_dest),
              _is_transfer_chain(is_transfer_chain),
              _send_query_statistics_with_every_batch(send_query_statistics_with_every_batch) {}

    virtual ~Channel() {
        if (_closure != nullptr && _closure->unref()) {
            delete _closure;
        }
        // release this before request desctruct
        _brpc_request.release_finst_id();
    }

    // Initialize channel.
    // Returns OK if successful, error indication otherwise.
    Status init(RuntimeState* state);

    // Copies a single row into this channel's output buffer and flushes buffer
    // if it reaches capacity.
    // Returns error status if any of the preceding rpcs failed, OK otherwise.
    //Status add_row(TupleRow* row);

    // Asynchronously sends a row batch.
    // Returns the status of the most recently finished transmit_data
    // rpc (or OK if there wasn't one that hasn't been reported yet).
    // if batch is nullptr, send the eof packet
    Status send_batch(PRowBatch* batch, bool eos = false);

    // Flush buffered rows and close channel. This function don't wait the response
    // of close operation, client should call close_wait() to finish channel's close.
    // We split one close operation into two phases in order to make multiple channels
    // can run parallel.
    Status close(RuntimeState* state);

    // Get close wait's response, to finish channel close operation.
    Status close_wait(RuntimeState* state);

    int64_t num_data_bytes_sent() const { return _num_data_bytes_sent; }

    PRowBatch* pb_batch() { return &_pb_batch; }

    std::string get_fragment_instance_id_str() {
        UniqueId uid(_fragment_instance_id);
        return uid.to_string();
    }

    TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

private:
    inline Status _wait_last_brpc() {
        if (_closure == nullptr) return Status::OK();
        auto cntl = &_closure->cntl;
        brpc::Join(cntl->call_id());
        if (cntl->Failed()) {
            std::string err = fmt::format(
                    "failed to send brpc batch, error={}, error_text={}, client: {}",
                    berror(cntl->ErrorCode()), cntl->ErrorText(), BackendOptions::get_localhost());
            LOG(WARNING) << err;
            return Status::ThriftRpcError(err);
        }
        return Status::OK();
    }

private:
    // Serialize _batch into _thrift_batch and send via send_batch().
    // Returns send_batch() status.
    Status send_current_batch(bool eos = false);
    Status close_internal();

    VDataSender* _parent;
    int _buffer_size;

    const RowDescriptor& _row_desc;
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // the number of TRowBatch.data bytes sent successfully
    int64_t _num_data_bytes_sent;
    int64_t _packet_seq;

    // we're accumulating rows into this batch
    //boost::scoped_ptr<RowBatch> _batch;
    std::unique_ptr<Block> _block;

    bool _need_close;
    int _be_number;

    TNetworkAddress _brpc_dest_addr;

    PUniqueId _finst_id;
    PRowBatch _pb_batch;
    PTransmitDataParams _brpc_request;
    PBackendService_Stub* _brpc_stub = nullptr;
    RefCountClosure<PTransmitDataResult>* _closure = nullptr;
    int32_t _brpc_timeout_ms = 500;
    // whether the dest can be treated as query statistics transfer chain.
    bool _is_transfer_chain;
    bool _send_query_statistics_with_every_batch;

    size_t _capacity;
};
} // namespace vectorized
} // namespace doris
