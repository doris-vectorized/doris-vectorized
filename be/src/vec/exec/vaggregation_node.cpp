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

#include "vec/exec/vaggregation_node.h"

#include <memory>

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "util/defer_op.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(NULL),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(NULL),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _agg_data(),
          _build_timer(nullptr),
          _exec_timer(nullptr),
          _merge_timer(nullptr) {}

AggregationNode::~AggregationNode() = default;

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(
                AggFnEvaluator::create(_pool, tnode.agg_node.aggregate_functions[i], &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _exec_timer = ADD_TIMER(runtime_profile(), "ExecTime");
    _merge_timer = ADD_TIMER(runtime_profile(), "MergeTime");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    _mem_pool = std::make_unique<MemPool>(mem_tracker().get());

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
                                                          _mem_pool.get(), intermediate_slot_desc,
                                                          output_slot_desc, mem_tracker()));
    }

    // set profile timer to evaluators
    for (auto& evaluator : _aggregate_evaluators) {
        evaluator->set_timer(_exec_timer, _merge_timer);
    }

    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;

        const auto& agg_function = _aggregate_evaluators[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->alignOfData());
        _total_size_of_aggregate_states += agg_function->sizeOfData();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _aggregate_evaluators.size()) {
            size_t alignment_of_next_state =
                    _aggregate_evaluators[i + 1]->function()->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }

    if (_probe_expr_ctxs.empty()) {
        _agg_data.init(AggregatedDataVariants::Type::without_key);

        _agg_data.without_key = reinterpret_cast<AggregateDataPtr>(
                _mem_pool->allocate(_total_size_of_aggregate_states));

        _create_agg_status(_agg_data.without_key);

        if (_is_merge) {
            _executor.execute = std::bind<Status>(&AggregationNode::_merge_without_key, this,
                                                  std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggregationNode::_execute_without_key, this,
                                                  std::placeholders::_1);
        }

        if (_needs_finalize) {
            _executor.get_result = std::bind<Status>(&AggregationNode::_get_without_key_result,
                                                     this, std::placeholders::_1,
                                                     std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(&AggregationNode::_serialize_without_key, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        }

        _executor.close = std::bind<void>(&AggregationNode::_close_without_key, this);
    } else {
        _agg_data.init(AggregatedDataVariants::Type::serialized);
        if (_is_merge) {
            _executor.execute = std::bind<Status>(&AggregationNode::_merge_with_serialized_key,
                                                  this, std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggregationNode::_execute_with_serialized_key,
                                                  this, std::placeholders::_1);
        }

        if (_needs_finalize) {
            _executor.get_result = std::bind<Status>(
                    &AggregationNode::_get_with_serialized_key_result, this, std::placeholders::_1,
                    std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(
                    &AggregationNode::_serialize_with_serialized_key_result, this,
                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
        _executor.close = std::bind<void>(&AggregationNode::_close_with_serialized_key, this);
    }

    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }

    RETURN_IF_ERROR(_children[0]->open(state));

    bool eos = false;

    while (!eos) {
        Block block;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next(state, &block, &eos));
        if (block.rows() == 0) {
            continue;
        }
        RETURN_IF_ERROR(_executor.execute(&block));
    }

    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented Aggregation Node::get_next scalar");
}

Status AggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    block->clear();
    RETURN_IF_ERROR(_executor.get_result(state, block, eos));
    if (_vconjunct_ctx_ptr) {
        int result_column_id = -1;
        int orig_columns = block->columns();
        (*bt)->execute(block, &result_column_id);
        Block::filter_block(block, result_column_id, orig_columns);
    }
    return Status::OK();
}

Status AggregationNode::close(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::close(state));
    VExpr::close(_probe_expr_ctxs, state);
    _executor.close();
    return Status::OK();
}

Status AggregationNode::_create_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_destory_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_get_without_key_result(RuntimeState* state, Block* block, bool* eos) {
    DCHECK(_agg_data.without_key != nullptr);

    *block = VectorizedUtils::create_empty_columnswithtypename(row_desc());

    int agg_size = _aggregate_evaluators.size();

    MutableColumns columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = _aggregate_evaluators[i]->function()->getReturnType();
        columns[i] = data_types[i]->createColumn();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        auto column = columns[i].get();
        _aggregate_evaluators[i]->insert_result_info(
                _agg_data.without_key + _offsets_of_aggregate_states[i], column);
    }

    const auto& block_schema = block->getColumnsWithTypeAndName();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            DCHECK(column_type->isNullable());
            DCHECK(((DataTypeNullable*)column_type.get())->getNestedType()->equals(*data_types[i]));
            DCHECK(!data_types[i]->isNullable());
            ColumnPtr ptr = std::move(columns[i]);
            ptr = makeNullable(ptr);
            columns[i] = std::move(*ptr).mutate();
        }
    }

    block->setColumns(std::move(columns));
    *eos = true;
    return Status::OK();
}

Status AggregationNode::_serialize_without_key(RuntimeState* state, Block* block, bool* eos) {
    DCHECK(_agg_data.without_key != nullptr);
    int agg_size = _aggregate_evaluators.size();

    MutableColumns value_columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);

    // will serialize data to string column
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = makeNullable(std::make_shared<DataTypeString>());
        value_columns[i] = data_types[i]->createColumn();
    }

    // TODO: we could use pod char as buffer instead of ostream ?
    std::ostringstream buf;
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->serialize(
                _agg_data.without_key + _offsets_of_aggregate_states[i], buf);
        auto str_buffer = buf.str();
        value_columns[i]->insertData(str_buffer.c_str(), str_buffer.length());
        buf.str("");
        buf.clear();
    }
    {
        ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = Block(data_with_schema);
    }

    block->setColumns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

Status AggregationNode::_execute_without_key(Block* block) {
    DCHECK(_agg_data.without_key != nullptr);
    LOG(WARNING) << "block rows:" << block->rows();
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->execute_single_add(
                block, _agg_data.without_key + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_merge_without_key(Block* block) {
    DCHECK(_agg_data.without_key != nullptr);
    std::unique_ptr<char[]> deserialize_buffer(new char[_total_size_of_aggregate_states]);
    int rows = block->rows();
    _create_agg_status(deserialize_buffer.get());
    DeferOp defer([&]() { _destory_agg_status(deserialize_buffer.get()); });
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        auto column = block->getByPosition(i).column;
        if (column->isNullable()) {
            column = ((ColumnNullable*)column.get())->getNestedColumnPtr();
        }

        for (int j = 0; j < rows; ++j) {
            std::string data_buffer;
            StringRef ref = column->getDataAt(j);
            data_buffer.assign(ref.data, ref.size);
            std::istringstream buf(data_buffer);

            _aggregate_evaluators[i]->function()->deserialize(
                    deserialize_buffer.get() + _offsets_of_aggregate_states[i], buf,
                    &_agg_arena_pool);

            _aggregate_evaluators[i]->function()->merge(
                    _agg_data.without_key + _offsets_of_aggregate_states[i],
                    deserialize_buffer.get() + _offsets_of_aggregate_states[i], &_agg_arena_pool);
        }
    }
    return Status::OK();
}

void AggregationNode::_close_without_key() {
    _destory_agg_status(_agg_data.without_key);
}

Status AggregationNode::_execute_with_serialized_key(Block* block) {
    DCHECK(!_probe_expr_ctxs.empty());
    // now we only support serialized key
    // TODO:
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& method = *_agg_data.serialized;

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
        key_columns[i] = block->getByPosition(result_column_id).column.get();
    }

    int rows = block->rows();
    PODArray<AggregateDataPtr> places(rows);

    AggState state(key_columns, {}, nullptr);

    /// For all rows.
    for (size_t i = 0; i < rows; ++i) {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, _agg_arena_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted()) {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = _agg_arena_pool.alignedAlloc(_total_size_of_aggregate_states,
                                                          _align_aggregate_states);
            _create_agg_status(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        } else
            aggregate_data = emplace_result.getMapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->execute_batch_add(block, _offsets_of_aggregate_states[i],
                                                    places.data(), &_agg_arena_pool);
    }

    return Status::OK();
}

Status AggregationNode::_get_with_serialized_key_result(RuntimeState* state, Block* block,
                                                        bool* eos) {
    DCHECK(_agg_data.serialized != nullptr);
    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    _agg_data.serialized->init_once();

    auto& method = *_agg_data.serialized;
    auto& data = _agg_data.serialized->data;
    auto& iter = _agg_data.serialized->iterator;

    block->clear();
    auto column_withschema = VectorizedUtils::create_columns_with_type_and_name(row_desc());

    int key_size = _probe_expr_ctxs.size();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        key_columns.emplace_back(column_withschema[i].type->createColumn());
    }
    MutableColumns value_columns;
    for (int i = key_size; i < column_withschema.size(); ++i) {
        value_columns.emplace_back(column_withschema[i].type->createColumn());
    }

    while (iter != data.end() && key_columns[0]->size() < state->batch_size()) {
        const auto& key = iter->getFirst();
        auto& mapped = iter->getSecond();
        method.insertKeyIntoColumns(key, key_columns, {});
        for (size_t i = 0; i < _aggregate_evaluators.size(); ++i)
            _aggregate_evaluators[i]->insert_result_info(mapped + _offsets_of_aggregate_states[i],
                                                         value_columns[i].get());

        ++iter;
    }

    *block = column_withschema;
    MutableColumns columns(block->columns());
    for (int i = 0; i < block->columns(); ++i) {
        if (i < key_size) {
            columns[i] = std::move(key_columns[i]);
        } else {
            columns[i] = std::move(value_columns[i - key_size]);
        }
    }
    block->setColumns(std::move(columns));
    if (iter == data.end()) {
        *eos = true;
    }
    return Status::OK();
}

Status AggregationNode::_serialize_with_serialized_key_result(RuntimeState* state, Block* block,
                                                              bool* eos) {
    DCHECK(_agg_data.serialized != nullptr);
    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    _agg_data.serialized->init_once();

    auto& method = *_agg_data.serialized;
    auto& data = _agg_data.serialized->data;
    auto& iter = _agg_data.serialized->iterator;

    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->createColumn());
    }

    // will serialize data to string column
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        value_data_types[i] = makeNullable(std::make_shared<DataTypeString>());
        value_columns[i] = value_data_types[i]->createColumn();
    }

    while (iter != data.end() && key_columns[0]->size() < state->batch_size()) {
        const auto& key = iter->getFirst();
        auto& mapped = iter->getSecond();
        // insert keys
        method.insertKeyIntoColumns(key, key_columns, {});

        std::ostringstream buf;
        // serialize values
        for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->function()->serialize(
                    mapped + _offsets_of_aggregate_states[i], buf);
            value_columns[i]->insertData(buf.str().c_str(), buf.str().length());
            buf.str("");
            buf.clear();
        }
        ++iter;
    }

    ColumnsWithTypeAndName columns_with_schema;
    for (int i = 0; i < key_size; ++i) {
        columns_with_schema.emplace_back(std::move(key_columns[i]),
                                         _probe_expr_ctxs[i]->root()->data_type(), "");
    }

    for (int i = 0; i < agg_size; ++i) {
        columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
    }

    *block = Block(columns_with_schema);
    if (iter == data.end()) {
        *eos = true;
    }
    return Status::OK();
}

Status AggregationNode::_merge_with_serialized_key(Block* block) {
    DCHECK(!_probe_expr_ctxs.empty());
    // now we only support serialized key
    // TODO:
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& method = *_agg_data.serialized;

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
        key_columns[i] = block->getByPosition(result_column_id).column.get();
    }

    int rows = block->rows();
    PODArray<AggregateDataPtr> places(rows);

    AggState state(key_columns, {}, nullptr);

    /// For all rows.
    for (size_t i = 0; i < rows; ++i) {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplaceKey(method.data, i, _agg_arena_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.isInserted()) {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.setMapped(nullptr);

            aggregate_data = _agg_arena_pool.alignedAlloc(_total_size_of_aggregate_states,
                                                          _align_aggregate_states);
            _create_agg_status(aggregate_data);

            emplace_result.setMapped(aggregate_data);
        } else
            aggregate_data = emplace_result.getMapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    std::unique_ptr<char[]> deserialize_buffer(new char[_total_size_of_aggregate_states]);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        auto column = block->getByPosition(i + key_size).column;
        if (column->isNullable()) {
            column = ((ColumnNullable*)column.get())->getNestedColumnPtr();
        }
        for (int j = 0; j < rows; ++j) {
            std::string data_buffer;
            StringRef ref = column->getDataAt(j);
            data_buffer.assign(ref.data, ref.size);
            std::istringstream buf(data_buffer);

            _aggregate_evaluators[i]->function()->deserialize(
                    deserialize_buffer.get() + _offsets_of_aggregate_states[i], buf,
                    &_agg_arena_pool);

            _aggregate_evaluators[i]->function()->merge(
                    places.data()[j] + _offsets_of_aggregate_states[i],
                    deserialize_buffer.get() + _offsets_of_aggregate_states[i], &_agg_arena_pool);
        }
    }
    return Status::OK();
}

void AggregationNode::_close_with_serialized_key() {
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& data = _agg_data.serialized->data;

    data.forEachValue([&](const auto& key, auto& mapped) { _destory_agg_status(mapped); });
}

} // namespace doris::vectorized
