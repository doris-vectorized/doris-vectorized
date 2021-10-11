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

#include "vec/exec/vset_operation_node.h"

#include "vec/exprs/vexpr.h"

namespace doris {
namespace vectorized {

//build hash table for operation node, intersect/except node
template <class HashTableContext>
struct HashTableBuild {
    HashTableBuild(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs,
                   VSetOperationNode* operation_node)
            : _rows(rows),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _operation_node(operation_node) {}

    Status operator()(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_build_raw_ptrs, _operation_node->_build_key_sz, nullptr);

        for (size_t k = 0; k < _rows; ++k) {
            auto emplace_result =
                    key_getter.emplace_key(hash_table_ctx.hash_table, k, _operation_node->_arena);
            if (k + 1 < _rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, k + 1, _operation_node->_arena);
            }
            if (emplace_result.is_inserted()) { //only inserted once as the same key, others skip
                new (&emplace_result.get_mapped()) Mapped({&_acquired_block, k});
            }
        }
        return Status::OK();
    }

private:
    const int _rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    VSetOperationNode* _operation_node;
};

VSetOperationNode::VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _has_init_hash_table(false),
          _const_expr_list_idx(0),
          _child_idx(0),
          _child_row_idx(0),
          _child_eos(false),
          _to_close_child_idx(-1) {}

Status VSetOperationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    for (auto& exprs : _const_expr_lists) {
        VExpr::close(exprs, state);
    }
    for (auto& exprs : _child_expr_lists) {
        VExpr::close(exprs, state);
    }
    return ExecNode::close(state);
}
Status VSetOperationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    DCHECK_EQ(_conjunct_ctxs.size(), 0);
    std::vector<std::vector<::doris::TExpr>> result_texpr_lists;
    std::vector<std::vector<::doris::TExpr>> const_texpr_lists;

    // Create const_expr_ctx_lists_ 、result_expr_ctx_lists_ from thrift exprs.
    if (tnode.node_type == TPlanNodeType::type::UNION_NODE) {
        const_texpr_lists = tnode.union_node.const_expr_lists;
        result_texpr_lists = tnode.union_node.result_expr_lists;
    } else if (tnode.node_type == TPlanNodeType::type::INTERSECT_NODE) {
        const_texpr_lists = tnode.intersect_node.const_expr_lists;
        result_texpr_lists = tnode.intersect_node.result_expr_lists;
    } else if (tnode.node_type == TPlanNodeType::type::EXCEPT_NODE) {
        const_texpr_lists = tnode.except_node.const_expr_lists;
        result_texpr_lists = tnode.except_node.result_expr_lists;
    } else {
        return Status::NotSupported("Not Implemented, Check The Operation Node.");
    }

    for (auto& texprs : const_texpr_lists) {
        std::vector<VExprContext*> ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, texprs, &ctxs));
        _const_expr_lists.push_back(ctxs);
    }

    for (auto& texprs : result_texpr_lists) {
        std::vector<VExprContext*> ctxs;
        RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, texprs, &ctxs));
        _child_expr_lists.push_back(ctxs);
    }

    for (auto ctx : _child_expr_lists[0]) {
        _build_not_ignore_null.push_back(ctx->root()->is_nullable());
    }
    for (auto ctx : _child_expr_lists[1]) {
        _probe_not_ignore_null.push_back(ctx->root()->is_nullable());
    }
    //now used for INTERSECT/EXCEPT NODE
    if (tnode.node_type != TPlanNodeType::type::UNION_NODE) {
        hash_table_init();  
        _has_init_hash_table = true;
    }
    return Status::OK();
}

Status VSetOperationNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    // open const expr lists.
    for (const std::vector<VExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }
    // open result expr lists.
    for (const std::vector<VExprContext*>& exprs : _child_expr_lists) {
        RETURN_IF_ERROR(VExpr::open(exprs, state));
    }

    // Ensures that rows are available for clients to fetch after this open() has
    // succeeded.
    if (!_children.empty()) RETURN_IF_ERROR(child(_child_idx)->open(state));

    if (_has_init_hash_table) {
        RETURN_IF_ERROR(hash_table_build(state));
    }
    return Status::OK();
}
Status VSetOperationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _materialize_exprs_evaluate_timer =
            ADD_TIMER(_runtime_profile, "MaterializeExprsEvaluateTimer");

    // Prepare const expr lists.
    for (const std::vector<VExprContext*>& exprs : _const_expr_lists) {
        RETURN_IF_ERROR(VExpr::prepare(exprs, state, row_desc(), expr_mem_tracker()));
    }

    // Prepare result expr lists.
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        RETURN_IF_ERROR(VExpr::prepare(_child_expr_lists[i], state, child(i)->row_desc(),
                                       expr_mem_tracker()));
    }
    return Status::OK();
}

void VSetOperationNode::hash_table_init() {
    if (_child_expr_lists[0].size() == 1 &&
        (!_build_not_ignore_null[0] || !_probe_not_ignore_null[0])) {
        // Single column optimization
        switch (_child_expr_lists[0][0]->root()->result_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            _hash_table_variants.emplace<I8HashTableContext>();
            break;
        case TYPE_SMALLINT:
            _hash_table_variants.emplace<I16HashTableContext>();
            break;
        case TYPE_INT:
        case TYPE_FLOAT:
            _hash_table_variants.emplace<I32HashTableContext>();
            break;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
            _hash_table_variants.emplace<I64HashTableContext>();
            break;
        case TYPE_LARGEINT:
        case TYPE_DATETIME:
        case TYPE_DATE:
        case TYPE_DECIMALV2:
            _hash_table_variants.emplace<I128HashTableContext>();
            break;
        default:
            _hash_table_variants.emplace<SerializedHashTableContext>();
        }
        return;
    }

    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_child_expr_lists[1].size());
    _build_key_sz.resize(_child_expr_lists[0].size());
    for (int i = 0; i < _child_expr_lists[0].size(); ++i) {
        const auto vexpr = _child_expr_lists[0][i]->root();
        const auto& data_type = vexpr->data_type();
        auto result_type = vexpr->result_type();
        has_null |= data_type->is_nullable();
        _build_key_sz[i] = get_real_byte_size(result_type);
        _probe_key_sz[i] = _build_key_sz[i];

        key_byte_size += _build_key_sz[i];
        if (has_variable_type(result_type)) {
            use_fixed_key = false;
            break;
        }
    }

    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                       sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<true>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<true>>();
            }
        } else {
            if (key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<false>>();
            } else if (key_byte_size <= sizeof(UInt128)) {
                _hash_table_variants.emplace<I128FixedKeyHashTableContext<false>>();
            } else {
                _hash_table_variants.emplace<I256FixedKeyHashTableContext<false>>();
            }
        }
    } else {
        _hash_table_variants.emplace<SerializedHashTableContext>();
    }
}
//build a hash table from child(0)
Status VSetOperationNode::hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(0)->open(state));
    Block block;
    bool eos = false;
    while (!eos) {
        block.clear();
        SCOPED_TIMER(_build_timer);
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next(state, &block, &eos));
        RETURN_IF_ERROR(process_build_block(block));
    }
    return Status::OK();
}

Status VSetOperationNode::process_build_block(Block& block) {
    size_t rows = block.rows();
    if (rows == 0) {
        return Status::OK();
    }
    auto& acquired_block = _acquire_list.acquire(std::move(block));
    vectorized::materialize_block_inplace(acquired_block);
    ColumnRawPtrs raw_ptrs(_child_expr_lists[0].size());

    // Get the key column that needs to be built
    Status st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return extract_build_join_column(acquired_block, raw_ptrs);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
                __builtin_unreachable();
            },
            _hash_table_variants);

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableBuild<HashTableCtxType> hash_table_build_process(rows, acquired_block,
                                                                              raw_ptrs, this);
                    st = hash_table_build_process(arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

Status VSetOperationNode::extract_build_join_column(Block& block, ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_expr_lists[0].size(); ++i) {
        int result_col_id = -1;
        // execute build column
        RETURN_IF_ERROR(_child_expr_lists[0][i]->execute(&block, &result_col_id));
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();

            if (_probe_not_ignore_null[i]) { // check the probe column is nullable or not
                raw_ptrs[i] = nullable;      // make two of them is consistent about nullable type
            } else {                         // one of them is non null, convert it to non null
                raw_ptrs[i] = &col_nested;   // in order to use one column optimization hashtable
            }
        } else {
            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

Status VSetOperationNode::extract_probe_join_column(Block& block, ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_expr_lists[1].size(); ++i) {
        int result_col_id = -1;
        // execute build column
        RETURN_IF_ERROR(_child_expr_lists[1][i]->execute(&block, &result_col_id));
        auto column = block.get_by_position(result_col_id).column.get();

        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();

            if (_build_not_ignore_null[i]) { //same as build column above
                raw_ptrs[i] = nullable;
            } else {
                raw_ptrs[i] = &col_nested;
            }
        } else {
            raw_ptrs[i] = column;
        }
    }
    return Status::OK();
}

void VSetOperationNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << VExpr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}
} // namespace vectorized
} // namespace doris
