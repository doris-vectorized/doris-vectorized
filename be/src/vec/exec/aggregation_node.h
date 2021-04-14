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
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class MemPool;

namespace vectorized {
class VExprContext;

/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData>
struct AggregationMethodSerialized {
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

    AggregationMethodSerialized() {}

    template <typename Other>
    AggregationMethodSerialized(const Other& other) : data(other.data) {}

    using State = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped>;

    static const bool low_cardinality_optimization = false;

    static void insertKeyIntoColumns(const StringRef& key, MutableColumns& key_columns,
                                     const Sizes&) {
        auto pos = key.data;
        for (auto& column : key_columns) pos = column->deserializeAndInsertFromArena(pos);
    }
};

using AggregatedDataWithoutKey = AggregateDataPtr;
using AggregatedDataWithStringKey = HashMapWithSavedHash<StringRef, AggregateDataPtr>;

struct AggregatedDataVariants {
    AggregatedDataVariants() = default;
    AggregatedDataVariants(const AggregatedDataVariants&) = delete;
    AggregatedDataVariants& operator=(const AggregatedDataVariants&) = delete;
    AggregatedDataWithoutKey without_key = nullptr;
    std::unique_ptr<AggregationMethodSerialized<AggregatedDataWithStringKey>> serialized;
    enum class Type {
        EMPTY = 0,
        without_key,
        serialized
        // TODO: add more key optimize types
    };

    Type _type = Type::EMPTY;

    void init(Type type) {
        _type = type;
        switch (_type) {
        case Type::serialized:
            serialized.reset(new AggregationMethodSerialized<AggregatedDataWithStringKey>());
            break;
        default:
            break;
        }
    }
};

using AggregatedDataVariantsPtr = std::shared_ptr<AggregatedDataVariants>;

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
    // group by k1,k2
    std::vector<VExprContext*> _probe_expr_ctxs;

    std::vector<AggFnEvaluator*> _aggregate_evaluators;

    // may be we don't have to know the tuple id
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc;

    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc;

    bool _needs_finalize;
    std::unique_ptr<MemPool> _mem_pool;

    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes _offsets_of_aggregate_states;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;

    AggregatedDataVariants _agg_data;
    // TODO:
    // add tracker here
    Arena _agg_arena_pool;

private:
    Status _create_agg_status(AggregateDataPtr data);
    Status _get_without_key_result(RuntimeState* state, Block* block, bool* eos);
    Status _execute_without_key(Block* block);

    Status _execute_with_serialized_key(Block* block);
    Status _get_with_serialized_key_result(RuntimeState* state, Block* block, bool* eos);
};
} // namespace vectorized
} // namespace doris
