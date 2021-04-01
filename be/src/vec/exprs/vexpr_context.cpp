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

#include "vec/exprs/vexpr_context.h"

#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
VExprContext::VExprContext(VExpr* expr)
        : _root(expr), _prepared(false), _opened(false), _closed(false) {}

doris::Status VExprContext::execute(doris::vectorized::Block* block, int* result_column_id) {
    return _root->execute(block, result_column_id);
}

doris::Status VExprContext::prepare(doris::RuntimeState* state,
                                    const doris::RowDescriptor& row_desc,
                                    const std::shared_ptr<doris::MemTracker>& tracker) {
    return _root->prepare(state, row_desc, this);
}
doris::Status VExprContext::open(doris::RuntimeState* state) {
    return _root->open(state, this);
}

void VExprContext::close(doris::RuntimeState* state) {
    return _root->close(state, this);
}

} // namespace doris::vectorized
