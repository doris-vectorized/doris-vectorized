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

#include "vec/exec/vintersect_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

template <class HashTableContext>
struct ProcessHashTableBuild2 {
    ProcessHashTableBuild2(int rows, Block& acquired_block, ColumnRawPtrs& build_raw_ptrs, VIntersectNode* join_node)
            : _rows(rows),
              _acquired_block(acquired_block),
              _build_raw_ptrs(build_raw_ptrs),
              _join_node(join_node) {}

    Status operator()(HashTableContext& hash_table_ctx) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_build_raw_ptrs, _join_node->_build_key_sz, nullptr);
        LOG(INFO)<<"ProcessHashTableBuild:_row: "<<_rows;
        for (size_t k = 0; k < _rows; ++k) {
            // TODO: make this as constexpr

            auto emplace_result = key_getter.emplace_key(hash_table_ctx.hash_table, k, _join_node->_arena);

            if (emplace_result.is_inserted()) {
                new (&emplace_result.get_mapped()) Mapped({&_acquired_block, k});  //RowRef(&_acquired_block, k)
                LOG(INFO)<<"is_inserted :"<<_acquired_block.get_by_position(0).to_string(k);
            } else {
                LOG(INFO)<<"emplace_result :"<<_acquired_block.get_by_position(0).to_string(0);
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.get_mapped().insert({&_acquired_block, k}, _join_node->_arena);
            }
        }

        return Status::OK();
    }

private:
    const int _rows;
    Block& _acquired_block;
    ColumnRawPtrs& _build_raw_ptrs;
    VIntersectNode* _join_node;
};


template <class HashTableContext>
struct ProcessHashTableProbe2 {
    ProcessHashTableProbe2(VIntersectNode* join_node, int batch_size, int probe_rows)
            : _join_node(join_node),
              _left_table_data_types(join_node->_left_table_data_types),
              _right_table_data_types(join_node->_right_table_data_types),
              _batch_size(batch_size),
              _probe_rows(probe_rows),
              _probe_block(join_node->_probe_block),
              _probe_index(join_node->_probe_index),
              _num_rows_returned(join_node->_num_rows_returned),
              _probe_raw_ptrs(join_node->_probe_columns),
              _arena(join_node->_arena) {}

    Status operator()(HashTableContext& hash_table_ctx,
                      MutableBlock& mutable_block, Block* output_block) {
        using KeyGetter = typename HashTableContext::State;
        using Mapped = typename HashTableContext::Mapped;

        KeyGetter key_getter(_probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        IColumn::Offsets offset_data;
        auto& mcol = mutable_block.mutable_columns();
        LOG(INFO)<<"mcol size: "<<mcol.size();
        offset_data.assign(_probe_rows, (uint32_t)0);

        //int right_col_idx = _left_table_data_types.size();
        int right_col_len = _right_table_data_types.size();
        int current_offset = 0;
        for(int i=0;i<_probe_raw_ptrs.size();++i)
        {
            LOG(INFO)<<"_probe_raw_ptrs["<<i<<"]->is_nullable(): "<<" "<<(_probe_raw_ptrs[i]->is_nullable());
            if (auto* nullable = check_and_get_column<ColumnNullable>(_probe_raw_ptrs[i])) {
                LOG(INFO)<<"check_and_get_column  nullable";
            }
            else
            {
                LOG(INFO)<<"Can NOT convert nullable";
            }
            if(is_column_nullable(*_probe_raw_ptrs[i]))
                LOG(INFO)<<"is_column_nullable true";
            else
                LOG(INFO)<<"is_column_nullable false";

        }
            

        for (; _probe_index < _probe_rows;) {
            // ignore null rows
            LOG(INFO)<<"_probe_rows: "<<_probe_rows<<"  _probe_index: "<<_probe_index;
            
            auto find_result = key_getter.find_key(hash_table_ctx.hash_table, _probe_index, _arena);
            LOG(INFO)<<"find_key find_result.is_found(): "<<find_result.is_found();


            if (find_result.is_found()) {
                auto& mapped = find_result.get_mapped();
                auto it = mapped.begin();
                //if(!(it->visited)){
                if(!mapped.is_visited()){
                    for (size_t j = 0; j < right_col_len; ++j) {
                        auto& column = *it->block->get_by_position(j).column;
                        for(int jj=0;jj<(it->block->columns());++jj)
                            LOG(INFO)<<"matched probe_data: "<<(it->block->get_by_position(jj)).to_string(0);
                        mcol[j]->insert_from(column, it->row_num);
                    }
                    ++current_offset;
                }
                //it->visited=true;
                mapped.set_visited();
            }

            offset_data[_probe_index++] = current_offset;

            if (current_offset >= _batch_size) {
                break;
            }
        }

        for (int i = _probe_index; i < _probe_rows; ++i) {
            offset_data[i] = current_offset;
        }

        output_block->swap(mutable_block.to_block());

        // for (int i = 0; i < right_col_idx; ++i) {
        //     auto& column = _probe_block.get_by_position(i).column;
        //     output_block->get_by_position(i).column = column->replicate(offset_data);
        // }

        // if (_join_node->_vconjunct_ctx_ptr) {
        //     int result_column_id = -1;
        //     int orig_columns = output_block->columns();
        //     (*_join_node->_vconjunct_ctx_ptr)->execute(output_block, &result_column_id);
        //     Block::filter_block(output_block, result_column_id, orig_columns);
        // }

        int64_t m = output_block->rows();
        _num_rows_returned += m;
        LOG(INFO)<<"output_block_rows: "<<m;
        return Status::OK();
    }

private:
    VIntersectNode* _join_node;
    const DataTypes& _left_table_data_types;
    const DataTypes& _right_table_data_types;
    const int _batch_size;
    const size_t _probe_rows;
    const Block& _probe_block;
    int& _probe_index;
    int64_t& _num_rows_returned;
    ColumnRawPtrs& _probe_raw_ptrs; //using ColumnRawPtrs = std::vector<const IColumn*>;
    Arena& _arena;
};

VIntersectNode::VIntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VSetOperationNode(pool, tnode, descs) {}
Status VIntersectNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::init(tnode, state));
    DCHECK(tnode.__isset.intersect_node);

    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        LOG(INFO)<<"debug string: "<<doris::vectorized::VExpr::debug_string(_child_expr_lists[i]);
        LOG(INFO) << "i: " << i;
        for (int j = 0; j < _child_expr_lists[i].size(); ++j) 
            LOG(INFO) << "j: " << j;
    }
    return Status::OK();
}

Status VIntersectNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::prepare(state));

    for (auto ctx : _child_expr_lists[0]) {
        _build_not_ignore_null.push_back(ctx->root()->is_nullable());
        //_build_not_ignore_null.push_back(0);
        LOG(INFO)<<"_build_not_ignore_null::prepare: "<<_build_not_ignore_null.back();
    }
    for (auto ctx : _child_expr_lists[1]) {
        _probe_not_ignore_null.push_back(ctx->root()->is_nullable());
        //_probe_not_ignore_null.push_back(0);
        LOG(INFO)<<"_probe_not_ignore_null::prepare: "<<_probe_not_ignore_null.back();
    }

    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());
    LOG(INFO)<<"_right_table_data_types: size: "<<_right_table_data_types.size()<<" _left_table_data_types: "<<_left_table_data_types.size();
    for(int i=0;i<_right_table_data_types.size();++i)
        LOG(INFO)<<"right: "<<_right_table_data_types[i]->get_name()<<" left: "<<_left_table_data_types[i]->get_name();

    _hash_table_init();
    return Status::OK();
}

Status VIntersectNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::open(state));

    RETURN_IF_ERROR(_hash_table_build(state));
    RETURN_IF_ERROR(child(1)->open(state));
    return Status::OK();
}

Status VIntersectNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    size_t probe_rows = _probe_block.rows();
    if (probe_rows == 0 || _probe_index == probe_rows) {
        _probe_index = 0;
        _probe_block.clear();
        int i=0;
        do {
            RETURN_IF_ERROR(child(1)->get_next(state, &_probe_block, eos));
            LOG(INFO)<<i++<<" probe: "<<_probe_block.columns()<<" rows: "<<_probe_block.rows()<<" ------------------------------";
            for(int k=0;k<_probe_block.rows();++k)
            {
                for(int j=0;j<_probe_block.columns();++j)
                    LOG(INFO)<<"data: "<<_probe_block.get_by_position(j).to_string(k);
            }
        } while (_probe_block.rows() == 0 && !(*eos));

        probe_rows = _probe_block.rows();
        if (probe_rows == 0) {
            *eos = true;
            return Status::OK();
        }

        int probe_expr_ctxs_sz = _child_expr_lists[1].size();
        _probe_columns.resize(probe_expr_ctxs_sz);

        for (int i = 0; i < probe_expr_ctxs_sz; ++i) {
            int result_id = -1;
            _child_expr_lists[1][i]->execute(&_probe_block, &result_id);
            DCHECK_GE(result_id, 0);
            LOG(INFO)<<"result_id: "<<result_id;
            _probe_columns[i] = _probe_block.get_by_position(result_id).column.get();
        }

        Status st = std::visit(
                [&](auto&& arg) -> Status {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        return extract_probe_join_column( _probe_block, _probe_columns);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                    __builtin_unreachable();
                },
                _hash_table_variants);

        RETURN_IF_ERROR(st);
    }

    MutableBlock mutable_block(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    output_block->clear();

    Status st;

    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        ProcessHashTableProbe2<HashTableCtxType> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);

                        st = process_hashtable_ctx(arg,
                                                   mutable_block, output_block);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

Status VIntersectNode::close(RuntimeState* state) {
    return VSetOperationNode::close(state);
}

void VIntersectNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << " _child_expr_lists=[";
    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        *out << VExpr::debug_string(_child_expr_lists[i]) << ", ";
    }
    *out << "] \n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")" << std::endl;
}

Status VIntersectNode::_hash_table_build(RuntimeState* state) {
    RETURN_IF_ERROR(child(0)->open(state));
    Block block;
    int i=0;
    bool eos = false;
    while (!eos) {
        block.clear();
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next(state, &block, &eos));
        LOG(INFO)<<"i is: "<<i++<<" build: "<<block.columns()<<" rows: "<<block.rows()<<" ----------------------------";
        for(int k=0;k<block.rows();++k)
            for(int j=0;j<block.columns();++j)
                LOG(INFO)<<"data: "<<block.get_by_position(j).to_string(k);
        RETURN_IF_ERROR(_process_build_block(block));
    }
    return Status::OK();
}

Status VIntersectNode::_process_build_block(Block& block) {
    size_t rows = block.rows();
    if (rows == 0) {
        LOG(INFO)<<"_process_build_block rows==0 return";
        return Status::OK();
    }
    LOG(INFO)<<"_process_build_block: columns: "<<block.columns()<<" rows: "<<block.rows();
    auto& acquired_block = _acquire_list.acquire(std::move(block));

    vectorized::materialize_block_inplace(acquired_block);

    ColumnRawPtrs raw_ptrs(_child_expr_lists[0].size());

    // Get the key column that needs to be built
    Status st = std::visit(
            [&](auto&& arg) -> Status {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    return extract_build_join_column(acquired_block,raw_ptrs);
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
                        ProcessHashTableBuild2<HashTableCtxType> hash_table_build_process(
                                rows, acquired_block, raw_ptrs, this);

                        st = hash_table_build_process(arg);
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);

    return st;
}

Status VIntersectNode::extract_build_join_column(Block& block, 
                                            ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_expr_lists[0].size(); ++i) {
        int result_col_id = -1;
        // execute build column
        RETURN_IF_ERROR(_child_expr_lists[0][i]->execute(&block, &result_col_id));
        LOG(INFO)<<"_build_not_ignore_null::build[i]: "<<i<<" "<<_build_not_ignore_null[i];

        auto column = block.get_by_position(result_col_id).column.get();
        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) 
        {
                auto& col_nested = nullable->get_nested_column();

                if (_build_not_ignore_null[i]) {
                    raw_ptrs[i] = nullable;
                } else {
                    LOG(INFO)<<"build: I am update_null_map";
                    raw_ptrs[i] = &col_nested;
                }
        } 
        else 
        {
            raw_ptrs[i] = _probe_not_ignore_null[i] ?
                    make_nullable(block.get_by_position(result_col_id).column, false).get() : column;
            LOG(INFO)<<"build: NotNullable: "<<_probe_not_ignore_null[i];
            LOG(INFO)<<"build: NotNullable: build_data: "<<block.get_by_position(result_col_id).to_string(0);
        }

    }
    return Status::OK();
}
Status VIntersectNode::extract_probe_join_column(Block& block, 
                                            ColumnRawPtrs& raw_ptrs) {
    for (size_t i = 0; i < _child_expr_lists[1].size(); ++i) {
        int result_col_id = -1;
        // execute build column
        RETURN_IF_ERROR(_child_expr_lists[1][i]->execute(&block, &result_col_id));
        LOG(INFO)<<"_probe_not_ignore_null::probe[i]: "<<i<<" "<<_probe_not_ignore_null[i];
        auto column = block.get_by_position(result_col_id).column.get();
        if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            auto& col_nested = nullable->get_nested_column();

            LOG(INFO)<<"probe could nullable: data: "<<block.get_by_position(result_col_id).to_string(0);;

            if (_build_not_ignore_null[i]) {
                raw_ptrs[i] = nullable;
            } else {
                LOG(INFO)<<"probe: I am update_null_map";

                raw_ptrs[i] = &col_nested;
            }
        } else {
            if(_build_not_ignore_null[i]==1)
            {
                LOG(INFO)<<"probe: NotNullable: "<<_build_not_ignore_null[i];
                auto column_ptr = make_nullable(block.get_by_position(result_col_id).column, false);
                block.insert({column_ptr, make_nullable(block.get_by_position(result_col_id).type), ""});
                column = column_ptr.get();
            }
            raw_ptrs[i] = column;
            if(is_column_nullable(*raw_ptrs[i]))
                LOG(INFO)<<"is_column_nullable make_nullable true";
            else
                LOG(INFO)<<"is_column_nullable make_nullable false";
            LOG(INFO)<<"probe: NotNullable: probe_data: "<<block.get_by_position(result_col_id).to_string(0);
        }
        
    }
    return Status::OK();
}
void VIntersectNode::_hash_table_init() {
    _hash_table_variants.emplace<SerializedHashTableContext>();
    /*
    if (_child_expr_lists[0].size() == 1 && !_build_not_ignore_null[0]) {
        // Single column optimization
        LOG(INFO)<<_child_expr_lists[0][0]->root()->result_type();
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
    */
    bool use_fixed_key = true;
    bool has_null = false;
    int key_byte_size = 0;

    _probe_key_sz.resize(_child_expr_lists[1].size());
    _build_key_sz.resize(_child_expr_lists[0].size());
    LOG(INFO)<<"_probe_key_sz: size: "<<_probe_key_sz.size()<<" _build_key_sz: "<<_build_key_sz.size();
    for (int i = 0; i < _child_expr_lists[0].size(); ++i) {
        const auto vexpr = _child_expr_lists[0][i]->root();
        const auto& data_type = vexpr->data_type();
        auto result_type = vexpr->result_type();
        LOG(INFO)<<"vexpr->: "<<vexpr->debug_string();
        has_null |= data_type->is_nullable();
        _build_key_sz[i] = get_real_byte_size(result_type);
        _probe_key_sz[i] = _build_key_sz[i];

        key_byte_size += _build_key_sz[i];

        if (has_variable_type(result_type)) {
            use_fixed_key = false;
            break;
        }
    }
    /*
    if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
        use_fixed_key = false;
    }

    if (use_fixed_key) {
        // TODO: may we should support uint256 in the future
        if (has_null) {
            if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                _hash_table_variants.emplace<I64FixedKeyHashTableContext<true>>();
            } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <= sizeof(UInt128)) {
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
    */
}
} // namespace vectorized
} // namespace doris
