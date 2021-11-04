#include "vec/exec/ves_http_scan_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/types.h"
#include "vec/exprs/vexpr_context.h"
namespace doris::vectorized {

VEsHttpScanNode::VEsHttpScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : EsHttpScanNode(pool, tnode, descs) {
    _vectorized = true;
}

VEsHttpScanNode::~VEsHttpScanNode() {}

Status VEsHttpScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (state->is_cancelled()) {
        std::unique_lock<std::mutex> l(_block_queue_lock);
        if (update_status(Status::Cancelled("Cancelled"))) {
            _queue_writer_cond.notify_all();
        }
    }

    if (_scan_finished.load() || _eos) {
        *eos = true;
        return Status::OK();
    }

    std::shared_ptr<vectorized::Block> scanner_block;
    {
        std::unique_lock<std::mutex> l(_block_queue_lock);
        while (_process_status.ok() && !_runtime_state->is_cancelled() &&
               _num_running_scanners > 0 && _block_queue.empty()) {
            SCOPED_TIMER(_wait_scanner_timer);
            _queue_reader_cond.wait_for(l, std::chrono::seconds(1));
        }
        if (!_process_status.ok()) {
            // Some scanner process failed.
            return _process_status;
        }
        if (_runtime_state->is_cancelled()) {
            if (update_status(Status::Cancelled("Cancelled"))) {
                _queue_writer_cond.notify_all();
            }
            return _process_status;
        }
        if (!_block_queue.empty()) {
            scanner_block = _block_queue.front();
            _block_queue.pop_front();
        }
    }

    // All scanner has been finished, and all cached batch has been read
    if (scanner_block == nullptr) {
        _scan_finished.store(true);
        *eos = true;
        return Status::OK();
    }

    // notify one scanner
    _queue_writer_cond.notify_one();

    *block = *(scanner_block.get());
    _num_rows_returned += block->rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    // This is first time reach limit.
    // Only valid when query 'select * from table1 limit 20'
    if (reached_limit()) {
        int num_rows_over = _num_rows_returned - _limit;
        block->set_num_rows(block->rows() - num_rows_over);
        _num_rows_returned -= num_rows_over;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);

        _scan_finished.store(true);
        _queue_writer_cond.notify_all();
        LOG(INFO) << "VEsHttpScanNode ReachedLimit.";
        *eos = true;
    } else {
        *eos = false;
    }

    return Status::OK();
}

Status VEsHttpScanNode::scanner_scan(std::unique_ptr<VEsHttpScanner> scanner) {
    RETURN_IF_ERROR(scanner->open());
    bool scanner_eof = false;

    const int batch_size = _runtime_state->batch_size();
    std::unique_ptr<MemPool> tuple_pool(new MemPool(mem_tracker().get()));
    size_t slot_num = _tuple_desc->slots().size();

    while (!scanner_eof) {
        std::shared_ptr<vectorized::Block> block(new vectorized::Block());
        std::vector<vectorized::MutableColumnPtr> columns(slot_num);
        for (int i = 0; i < slot_num; i ++) {
            columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
        }
        while (columns[0]->size() < batch_size && !scanner_eof) {
            RETURN_IF_CANCELLED(_runtime_state);

            // If we have finished all works
            if (_scan_finished.load()) {
                return Status::OK();
            }

            // Get from scanner
            RETURN_IF_ERROR(scanner->get_next(columns, tuple_pool.get(), &scanner_eof, _docvalue_context));
        }

        if (columns[0]->size() > 0) {
            auto n_columns = 0;
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
            if (_vconjunct_ctx_ptr != nullptr) {
                int result_column_id = -1;
                (*_vconjunct_ctx_ptr)->execute(block.get(), &result_column_id);
                Block::filter_block(block.get(), result_column_id, _tuple_desc->slots().size());
            }

            std::unique_lock<std::mutex> l(_block_queue_lock);
            while (_process_status.ok() && !_scan_finished.load() &&
                   !_runtime_state->is_cancelled() &&
                   _block_queue.size() >= _max_buffered_batches) {
                _queue_writer_cond.wait_for(l, std::chrono::seconds(1));
            }
            // Process already set failed, so we just return OK
            if (!_process_status.ok()) {
                return Status::OK();
            }
            // Scan already finished, just return
            if (_scan_finished.load()) {
                return Status::OK();
            }
            // Runtime state is canceled, just return cancel
            if (_runtime_state->is_cancelled()) {
                return Status::Cancelled("Cancelled");
            }
            _block_queue.push_back(block);

            // Notify reader to
            _queue_reader_cond.notify_one();
        }
    }

    return Status::OK();
}

Status VEsHttpScanNode::close(RuntimeState* state) {
    EsHttpScanNode::close(state);
    _block_queue.clear();
    return _process_status;
}

} // namespace doris::vectorized