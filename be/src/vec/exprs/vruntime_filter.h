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

#pragma once

#include "exprs/runtime_filter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "vec/exec/join/join_op.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

class VRuntimeFilterSlots {
public:
    VRuntimeFilterSlots(std::vector<VExprContext*>& prob_expr_ctxs,
                        std::vector<VExprContext*>& build_expr_ctxs,
                        const std::vector<TRuntimeFilterDesc>& runtime_filter_descs)
            : _probe_expr_context(prob_expr_ctxs),
              _build_expr_context(build_expr_ctxs),
              _runtime_filter_descs(runtime_filter_descs) {}

    Status init(RuntimeState* state, int64_t hash_table_size);

    void insert(std::unordered_map<const Block*, std::vector<int>>& datas);

    bool empty() { return !_runtime_filters.size(); }

    // should call this method after insert
    void ready_for_publish();
    // publish runtime filter
    void publish();

private:
    std::vector<VExprContext*>& _probe_expr_context;
    std::vector<VExprContext*>& _build_expr_context;
    const std::vector<TRuntimeFilterDesc>& _runtime_filter_descs;
    std::map<int, std::list<IRuntimeFilter*>> _runtime_filters;
};

} // namespace vectorized
} // namespace doris
