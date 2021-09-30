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

#include "vec/exprs/vruntime_filter.h"

namespace doris {
namespace vectorized {

Status VRuntimeFilterSlots::init(RuntimeState* state, int64_t hash_table_size) {
    DCHECK(_probe_expr_context.size() == _build_expr_context.size());
    // runtime filter effect stragety
    // 1. we will ignore IN filter when hash_table_size is too big
    // 2. we will ignore BLOOM filter and MinMax filter when hash_table_size
    // is too small and IN filter has effect

    std::map<int, bool> has_in_filter;

    auto ignore_filter = [state](int filter_id) {
        IRuntimeFilter* consumer_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_id, &consumer_filter);
        DCHECK(consumer_filter != nullptr);
        consumer_filter->set_ignored();
        consumer_filter->signal();
    };
    for (auto& filter_desc : _runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(filter_desc.filter_id,
                                                                         &runtime_filter));
        DCHECK(runtime_filter != nullptr);
        DCHECK(runtime_filter->expr_order() >= 0);
        DCHECK(runtime_filter->expr_order() < _probe_expr_context.size());

        // do not create 'in filter' when hash_table size over limit
        bool over_max_in_num = (hash_table_size >= state->runtime_filter_max_in_num());

        bool is_in_filter = (runtime_filter->type() == RuntimeFilterType::IN_FILTER);

        // do not create 'bloom filter' and 'minmax filter' when 'in filter' has created
        bool pass_not_in = (has_in_filter[runtime_filter->expr_order()] &&
                            !runtime_filter->has_remote_target());

        if (over_max_in_num == is_in_filter && (is_in_filter || pass_not_in)) {
            ignore_filter(filter_desc.filter_id);
            continue;
        }

        has_in_filter[runtime_filter->expr_order()] =
                (runtime_filter->type() == RuntimeFilterType::IN_FILTER);
        _runtime_filters[runtime_filter->expr_order()].push_back(runtime_filter);
    }

    return Status::OK();
}

void VRuntimeFilterSlots::insert(std::unordered_map<const Block*, std::vector<int>>& datas) {
    for (int i = 0; i < _build_expr_context.size(); ++i) {
        auto iter = _runtime_filters.find(i);
        if (iter == _runtime_filters.end()) continue;

        int result_column_id = _build_expr_context[i]->get_last_result_column_id();
        for (auto it : datas) {
            auto& column = it.first->get_by_position(result_column_id).column;

            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& column_nested = nullable->get_nested_column();
                auto& column_nullmap = nullable->get_null_map_column();
                for (int row_num : it.second) {
                    if (column_nullmap.get_bool(row_num)) {
                        continue;
                    }
                    const auto& ref_data = column_nested.get_data_at(row_num);
                    for (auto filter : iter->second) {
                        filter->insert(ref_data);
                    }
                }

            } else {
                for (int row_num : it.second) {
                    const auto& ref_data = column->get_data_at(row_num);
                    for (auto filter : iter->second) {
                        filter->insert(ref_data);
                    }
                }
            }
        }
    }
}

void VRuntimeFilterSlots::publish() {
    for (int i = 0; i < _probe_expr_context.size(); ++i) {
        auto iter = _runtime_filters.find(i);
        if (iter != _runtime_filters.end()) {
            for (auto filter : iter->second) {
                filter->publish();
            }
        }
    }
    for (auto& pair : _runtime_filters) {
        for (auto filter : pair.second) {
            filter->publish_finally();
        }
    }
}

} // namespace vectorized
} // namespace doris
