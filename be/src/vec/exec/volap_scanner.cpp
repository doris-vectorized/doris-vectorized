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

#include "vec/exec/volap_scanner.h"

#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exprs/vexpr_context.h"

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

    do {
        block->clear();
        std::vector<vectorized::MutableColumnPtr> columns;
        for (auto slot : get_query_slots()) {
            columns.emplace_back(slot->get_empty_mutable_column());
        }
        while (true) {
            // block is full, break
            if (state->batch_size() <= columns[0]->size()) {
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

        if (_vconjunct_ctx != nullptr) {
            int result_column_id = -1;
            _vconjunct_ctx->execute(block, &result_column_id);
            Block::filter_block(block, result_column_id, _tuple_desc->slots().size());
        }
    } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold);

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
        case TYPE_VARCHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            (*columns)[i]->insertData(slice->data, slice->size);
            break;
        }
        case TYPE_OBJECT: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            // insertDefault()
            auto& target_column = (*columns)[i];
            target_column->insertDefault();
            BitmapValue* pvalue = nullptr;
            int pos = target_column->size() - 1;
            if (target_column->isNullable()) {
                auto& nullable_column = assert_cast<ColumnNullable&>(*target_column);
                auto& bitmap_column = assert_cast<ColumnBitmap&>(nullable_column.getNestedColumn());
                pvalue = &bitmap_column.getElement(pos);
            } else {
                auto& bitmap_column = assert_cast<ColumnBitmap&>(*target_column);
                pvalue = &bitmap_column.getElement(pos);
            }
            if (slice->size != 0) {
                BitmapValue value;
                value.deserialize(slice->data);
                *pvalue = std::move(value);
            } else {
                *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice->data));
            }
            break;
        }
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            if (slice->size != 0) {
                (*columns)[i]->insertData(slice->data, slice->size);
                // TODO: in vector exec engine, it is diffcult to set hll size = 0
                // so we have to serialize here. which will cause two problem
                //      1. some unnecessary mem malloc and delay mem release
                //      2. some unnecessary CPU cost in serialize
            } else {
                auto* dst_hll = reinterpret_cast<HyperLogLog*>(slice->data);
                std::string result(dst_hll->max_serialized_size(), '0');
                int size = dst_hll->serialize((uint8_t*)result.c_str());
                result.resize(size);
                (*columns)[i]->insertData(result.c_str(), size);
            }
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
            DateTimeValue date;
            date.from_olap_date(value);
            (*columns)[i]->insertData(reinterpret_cast<char *>(&date), slot_desc->slot_size());
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
