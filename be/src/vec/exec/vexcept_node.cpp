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

#include "vec/exec/vexcept_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

template <class HashTableContext>
struct HashTableProbeExcept {
    HashTableProbeExcept(VExceptNode* join_node, int batch_size, int probe_rows)
            : _join_node(join_node),
              _left_table_data_types(join_node->_left_table_data_types),
              _batch_size(batch_size),
              _probe_rows(probe_rows),
              _probe_block(join_node->_probe_block),
              _probe_index(join_node->_probe_index),
              _num_rows_returned(join_node->_num_rows_returned),
              _probe_raw_ptrs(join_node->_probe_columns),
              _arena(join_node->_arena),
              _rows_returned_counter(join_node->_rows_returned_counter) {}

    Status mark_data_in_hashtable(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        for (; _probe_index < _probe_rows;) {
            auto find_result = key_getter.find_key(hash_table_ctx.hash_table, _probe_index, _arena);
            if (_probe_index + 1 < _probe_rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, _probe_index + 1, _arena);
            }

            if (find_result.is_found()) { //if found, marked visited
                auto it = find_result.get_mapped().begin();
                if (!(it->visited)) {
                    it->visited = true;
                }
            }
            _probe_index++;
        }
        return Status::OK();
    }

    Status get_data_in_hashtable(HashTableContext& hash_table_ctx, MutableBlock& mutable_block,
                                 Block* output_block, bool* eos) {
        hash_table_ctx.init_once();
        auto& mcol = mutable_block.mutable_columns();
        int left_col_len = _left_table_data_types.size();
        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;

        for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
            auto it = iter->get_second().begin();
            if (!it->visited) { //have done probe, so no visited values it's the result
                block_size++;
                for (size_t j = 0; j < left_col_len; ++j) {
                    auto& column = *it->block->get_by_position(j).column;
                    mcol[j]->insert_from(column, it->row_num);
                }
            }
        }
        *eos = iter == hash_table_ctx.hash_table.end();
        output_block->swap(mutable_block.to_block());

        int64_t m = output_block->rows();
        COUNTER_UPDATE(_rows_returned_counter, m);
        _num_rows_returned += m;
        return Status::OK();
    }

private:
    VExceptNode* _join_node;
    const DataTypes& _left_table_data_types;
    const int _batch_size;
    const size_t _probe_rows;
    const Block& _probe_block;
    int& _probe_index;
    int64_t& _num_rows_returned;
    ColumnRawPtrs& _probe_raw_ptrs;
    Arena& _arena;
    RuntimeProfile::Counter* _rows_returned_counter;
};

VExceptNode::VExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VSetOperationNode(pool, tnode, descs) {}

Status VExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::init(tnode, state));
    DCHECK(tnode.__isset.except_node);
    return Status::OK();
}

Status VExceptNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::prepare(state));
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());
    return Status::OK();
}

Status VExceptNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::open(state));
    RETURN_IF_ERROR(child(1)->open(state));
    return Status::OK();
}

Status VExceptNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    SCOPED_TIMER(_probe_timer);
    size_t probe_rows = _probe_block.rows();
    if ((probe_rows == 0 || _probe_index == probe_rows) && !_probe_eos) {
        _probe_index = 0;
        _probe_block.clear();
        do {
            RETURN_IF_ERROR(child(1)->get_next(state, &_probe_block, &_probe_eos));
        } while (_probe_block.rows() == 0 && !_probe_eos);

        probe_rows = _probe_block.rows();
        if (probe_rows != 0) {
            int probe_expr_ctxs_sz = _child_expr_lists[1].size();
            _probe_columns.resize(probe_expr_ctxs_sz);

            Status st = std::visit(
                    [&](auto&& arg) -> Status {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                            return extract_probe_join_column(_probe_block, _probe_columns);
                        } else {
                            LOG(FATAL) << "FATAL: uninited hash table";
                        }
                        __builtin_unreachable();
                    },
                    _hash_table_variants);
            RETURN_IF_ERROR(st);
        }
    }

    Status st;
    if (_probe_index < _probe_block.rows()) {
        std::visit(
                [&](auto&& arg) {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        HashTableProbeExcept<HashTableCtxType> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.mark_data_in_hashtable(arg);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                },
                _hash_table_variants);
    } else if (_probe_eos) {
        MutableBlock mutable_block(
                VectorizedUtils::create_empty_columnswithtypename(child(0)->row_desc()));
        output_block->clear();
        std::visit(
                [&](auto&& arg) {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        HashTableProbeExcept<HashTableCtxType> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.get_data_in_hashtable(arg, mutable_block,
                                                                         output_block, eos);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                },
                _hash_table_variants);
    }
    return st;
}

Status VExceptNode::close(RuntimeState* state) {
    return VSetOperationNode::close(state);
}

} // namespace vectorized
} // namespace doris
