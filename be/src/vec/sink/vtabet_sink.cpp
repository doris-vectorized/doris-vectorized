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

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/sink/vtablet_sink.h"
#include "vec/core/block.h"

namespace doris {
namespace stream_load {

VOlapTableSink::VOlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& texprs, Status* status)
        : OlapTableSink(pool, row_desc, texprs, status) {
    // From the thrift expressions create the real exprs.
    vectorized::VExpr::create_expr_trees(pool, texprs, &_output_vexpr_ctxs);
    // Do not use the origin data scala expr, clear scala expr contexts
    _output_expr_ctxs.clear();
    _name = "VOlapTableSink";
}

Status VOlapTableSink::prepare(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _input_row_desc, _expr_mem_tracker));
    return OlapTableSink::prepare(state);
}

Status VOlapTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));
    return OlapTableSink::open(state);
}

Status VOlapTableSink::send(RuntimeState* state, vectorized::Block* input_block) {
    Status status = Status::OK();
    if (UNLIKELY(input_block->rows() == 0)) { return status; }

    if (_output_vexpr_ctxs.empty()) {
        if (UNLIKELY(_input_batch == nullptr || _input_batch->capacity() < input_block->rows())) {
            _input_batch.reset(new RowBatch(_input_row_desc,
                                            state->batch_size() > input_block->rows() ? state->batch_size()
                                                                                      : input_block->rows(),
                                            _mem_tracker.get()));
        } else {
            _input_batch->reset();
        }

        input_block->serialize(_input_batch.get(), _input_row_desc);
        return OlapTableSink::send(state, _input_batch.get());
    }

    // Do vectorized expr here to speed up load
    auto block = vectorized::VExprContext::get_output_block_after_execute_exprs(
            _output_vexpr_ctxs, *input_block, status);
    auto num_rows = block.rows();
    if (UNLIKELY(num_rows == 0)) { return status; }

    if (UNLIKELY(_output_batch->capacity() < block.rows())) {
        _output_batch.reset(new RowBatch(*_output_row_desc, block.rows(), _mem_tracker.get()));
    } else {
        _output_batch->reset();
    }

    block.serialize(_output_batch.get(), *_output_row_desc);
    return OlapTableSink::send(state, _output_batch.get());
}

Status VOlapTableSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) return _close_status;
    vectorized::VExpr::close(_output_vexpr_ctxs, state);
    return OlapTableSink::close(state, exec_status);
}

} // namespace stream_load
} // namespace doris

