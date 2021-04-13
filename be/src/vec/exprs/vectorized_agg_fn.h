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
#include "runtime/types.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
namespace vectorized {
class AggFnEvaluator {
public:
    static Status create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result);

    Status prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                   const SlotDescriptor* intermediate_slot_desc,
                   const SlotDescriptor* output_slot_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    // create/destroy AGG Data
    void create(AggregateDataPtr place);
    void destroy(AggregateDataPtr place);

    // agg_function
    // void execute_single_add(int row_size, AggregateDataPtr place);
    void execute_single_add(Block* block, AggregateDataPtr place);

    void insert_result_info(AggregateDataPtr place, IColumn* column);

    // void execute_add(int row_size, AggregateDataPtr* places, size_t place_offset);

    void execute_batch_add(Block* block, size_t offset, AggregateDataPtr* places, Arena* arena);

    DataTypePtr& data_type() { return _data_type; }

    const AggregateFunctionPtr& function() { return _function; }

private:
    const TFunction _fn;

    AggFnEvaluator(const TExprNode& desc);

    const TypeDescriptor _return_type;
    const TypeDescriptor _intermediate_type;

    const SlotDescriptor* _intermediate_slot_desc;
    const SlotDescriptor* _output_slot_desc;

    // input context
    std::vector<VExprContext*> _input_exprs_ctxs;

    DataTypePtr _data_type;

    AggregateFunctionPtr _function;

    std::string _expr_name;
};
} // namespace vectorized

} // namespace doris
