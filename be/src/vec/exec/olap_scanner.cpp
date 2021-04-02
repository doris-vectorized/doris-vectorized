// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/olap_scanner.h"

#include "vec/core/block.h"
#include "vec/exec/olap_scan_node.h"

namespace doris::vectorized {

VOlapScanner::VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                           bool need_agg_finalize, const TPaloScanRange& scan_range,
                           const std::vector<OlapScanRange*>& key_ranges)
        : OlapScanner(runtime_state, parent, aggregation, need_agg_finalize, scan_range,
                      key_ranges),
          _runtime_state(runtime_state),
          _parent(parent),
          _profile(parent->runtime_profile()) {}

VOlapScanner::~VOlapScanner() {}

Status VOlapScanner::get_block(RuntimeState* state, vectorized::Block* block, bool* eof) {
    auto tracker = MemTracker::CreateTracker(state->fragment_mem_tracker()->limit(),
                                             "VOlapScanner:" + print_id(state->query_id()),
                                             state->fragment_mem_tracker());
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    auto agg_object_pool = std::make_unique<ObjectPool>();
    block->clear();
    {
        std::vector<vectorized::MutableColumnPtr> columns;
        for (auto slot : get_query_slots()) {
            columns.emplace_back(slot->get_empty_mutable_column());
        }
        while (true) {
            // block is full, break
            if (state->batch_size() <= block->rows()) {
                _update_realtime_counter();
                break;
            }
            // Read one row from reader
            auto res = _reader->next_row_with_aggregation(&_read_row_cursor, mem_pool.get(),
                                                          agg_object_pool.get(), eof);
            if (res != OLAP_SUCCESS) {
                std::stringstream ss;
                ss << "Internal Error: read storage fail. res=" << res
                   << ", tablet=" << _tablet->full_name()
                   << ", backend=" << BackendOptions::get_localhost();
                return Status::InternalError(ss.str());
            }
            // If we reach end of this scanner, break
            if (UNLIKELY(*eof)) {
                break;
            }

            _num_rows_read++;

            _convert_row_to_block(&columns);
            VLOG_ROW << "VOlapScanner input row: " << _read_row_cursor.to_string();

            //   // 3.4 Set tuple to RowBatch(not committed)
            //   int row_idx = batch->add_row();
            //   TupleRow* row = batch->get_row(row_idx);
            //   row->set_tuple(_tuple_idx, tuple);

            //   do {
            //     // 3.5.1 Using direct conjuncts to filter data
            //     if (_eval_conjuncts_fn != nullptr) {
            //       if (!_eval_conjuncts_fn(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
            //         // check direct conjuncts fail then clear tuple for reuse
            //         // make sure to reset null indicators since we're overwriting
            //         // the tuple assembled for the previous row
            //         tuple->init(_tuple_desc->byte_size());
            //         break;
            //       }
            //     } else {
            //       if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
            //         // check direct conjuncts fail then clear tuple for reuse
            //         // make sure to reset null indicators since we're overwriting
            //         // the tuple assembled for the previous row
            //         tuple->init(_tuple_desc->byte_size());
            //         break;
            //       }
            //     }

            //     // 3.5.2 Using pushdown conjuncts to filter data
            //     if (_use_pushdown_conjuncts) {
            //       if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[_direct_conjunct_size],
            //                                     _conjunct_ctxs.size() - _direct_conjunct_size,
            //                                     row)) {
            //         // check pushdown conjuncts fail then clear tuple for reuse
            //         // make sure to reset null indicators since we're overwriting
            //         // the tuple assembled for the previous row
            //         tuple->init(_tuple_desc->byte_size());
            //         _num_rows_pushed_cond_filtered++;
            //         break;
            //       }
            //     }

            //     // Copy string slot
            //     for (auto desc : _string_slots) {
            //       StringValue* slot = tuple->get_string_slot(desc->tuple_offset());
            //       if (slot->len != 0) {
            //         uint8_t* v = batch->tuple_data_pool()->allocate(slot->len);
            //         memory_copy(v, slot->ptr, slot->len);
            //         slot->ptr = reinterpret_cast<char*>(v);
            //       }
            //     }

            //     // the memory allocate by mem pool has been copied,
            //     // so we should release these memory immediately
            //     mem_pool->clear();

            //     if (VLOG_ROW_IS_ON) {
            //       VLOG_ROW << "OlapScanner output row: " << Tuple::to_string(tuple, *_tuple_desc);
            //     }

            //     // check direct && pushdown conjuncts success then commit tuple
            //     batch->commit_last_row();
            //     char* new_tuple = reinterpret_cast<char*>(tuple);
            //     new_tuple += _tuple_desc->byte_size();
            //     tuple = reinterpret_cast<Tuple*>(new_tuple);

            //     // compute pushdown conjuncts filter rate
            //     if (_use_pushdown_conjuncts) {
            //       // check this rate after
            //       if (_num_rows_read > 32768) {
            //         int32_t pushdown_return_rate =
            //             _num_rows_read * 100 /
            //                 (_num_rows_read + _num_rows_pushed_cond_filtered);
            //         if (pushdown_return_rate >
            //             config::doris_max_pushdown_conjuncts_return_rate) {
            //           _use_pushdown_conjuncts = false;
            //           VLOG_CRITICAL << "Stop Using PushDown Conjuncts. "
            //                         << "PushDownReturnRate: " << pushdown_return_rate << "%"
            //                         << " MaxPushDownReturnRate: "
            //                         << config::doris_max_pushdown_conjuncts_return_rate << "%";
            //         }
            //       }
            //     }
            //   } while (false);

            if (raw_rows_read() >= raw_rows_threshold) {
                break;
            }
        }
        auto n_columns = 0;
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(columns[n_columns++]->getPtr(),
                                                slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
        VLOG_ROW << "VOlapScanner output rows: " << block->rows();
    }
    return Status::OK();
}

void VOlapScanner::_convert_row_to_block(std::vector<vectorized::MutableColumnPtr>* columns) {
    size_t slots_size = _query_slots.size();
    for (int i = 0; i < slots_size; ++i) {
        SlotDescriptor* slot_desc = _query_slots[i];
        auto cid = _return_columns[i];
        if (_read_row_cursor.is_null(cid)) {
            (*columns)[i]->insertData(nullptr, 0);
            continue;
        }
        char* ptr = (char*)_read_row_cursor.cell_ptr(cid);
        size_t len = _read_row_cursor.column_size(cid);
        switch (slot_desc->type().type) {
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            (*columns)[i]->insertData(slice->data, strnlen(slice->data, slice->size));
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_OBJECT:
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            (*columns)[i]->insertData(slice->data, slice->size);
            break;
        }
        case TYPE_DECIMAL: {
            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            DecimalValue data(int_value, frac_value);
            (*columns)[i]->insertData(reinterpret_cast<char*>(&data), slot_desc->slot_size());
            break;
        }
        case TYPE_DECIMALV2: {
            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            DecimalV2Value data(int_value, frac_value);
            (*columns)[i]->insertData(reinterpret_cast<char*>(&data), slot_desc->slot_size());
            break;
        }
        case TYPE_DATETIME: {
            uint64_t value = *reinterpret_cast<uint64_t*>(ptr);
            DateTimeValue data(value);
            (*columns)[i]->insertData(reinterpret_cast<char*>(&data), slot_desc->slot_size());
            break;
        }
        case TYPE_DATE: {
            uint64_t value = 0;
            value = *(unsigned char*)(ptr + 2);
            value <<= 8;
            value |= *(unsigned char*)(ptr + 1);
            value <<= 8;
            value |= *(unsigned char*)(ptr);
            DateTimeValue data(value);
            (*columns)[i]->insertData(reinterpret_cast<char*>(&data), slot_desc->slot_size());
            break;
        }
        default: {
            (*columns)[i]->insertData(ptr, len);
            break;
        }
        }
    }
}

} // namespace doris::vectorized
