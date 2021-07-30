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
#include "exec/exec_node.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class MemPool;

namespace vectorized {
class VExprContext;

using AggregatedDataWithoutKey = AggregateDataPtr;

struct AggregatedDataVariants {
    AggregatedDataVariants(const AggregatedDataVariants&) = delete;
    AggregatedDataVariants& operator=(const AggregatedDataVariants&) = delete;
    AggregatedDataWithoutKey without_key = nullptr;
    enum class Type {
        EMPTY = 0,
        without_key
        // TODO: add more keys
    };
    Type type = Type::EMPTY;
};

// not support spill
class AggregationNode : public ::doris::ExecNode {
public:
    using Sizes = std::vector<size_t>;

    AggregationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~AggregationNode();
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, Block* block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    // TODO: provide a hash table

    // used for input
    std::vector<VExprContext*> _probe_expr_ctxs;
    // used for group by key
    std::vector<VExprContext*> _build_expr_ctxs;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    std::unique_ptr<MemPool> _mem_pool;

    // TODO:
    AggregateDataPtr _single_data_ptr;
    std::unique_ptr<Block> _single_output_block;

    size_t _align_aggregate_states = 1;
    Sizes _offsets_of_aggregate_states; /// The offset to the n-th aggregate function in a row of aggregate functions.
    size_t _total_size_of_aggregate_states =
            0; /// The total size of the row from the aggregate functions.
private:
    Status _create_agg_status(AggregateDataPtr data);
};
} // namespace vectorized
} // namespace doris
