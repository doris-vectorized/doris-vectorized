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
struct ProcessHashTableProbe3 {
    ProcessHashTableProbe3(VExceptNode* join_node, int batch_size, int probe_rows)
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

        int current_offset = 0;
        int left_col_len = _left_table_data_types.size();

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

            if (_probe_index + 1 < _probe_rows) {
                key_getter.prefetch(hash_table_ctx.hash_table, _probe_index + 1, _arena);
            }
            if (find_result.is_found()) {
                auto& mapped = find_result.get_mapped();
                auto it = mapped.begin();
                //if(!(it->visited)) {
                if(!mapped.is_visited()){
                //for (auto it = mapped.begin(); it.ok()&&(!it->visited); ++it) {
                    for (size_t j = 0; j < left_col_len; ++j) {
                        for(int jj=0;jj<(it->block->columns());++jj)
                            LOG(INFO)<<"matched probe_data: "<<(it->block->get_by_position(jj)).to_string(it->row_num);
                    }
                    ++current_offset;
                    //it->visited=true;
                }
                mapped.set_visited();
            }
            _probe_index++;

            if (current_offset >= _batch_size) {
                break;
            }
        }

        return Status::OK();
    }

Status get_data_in_hashtable(HashTableContext& hash_table_ctx, MutableBlock& mutable_block, Block* output_block, bool* eos) {
        hash_table_ctx.init_once();

        auto& mcol = mutable_block.mutable_columns();
        int left_col_len =  _left_table_data_types.size();

        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;
        for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
            auto& mapped = iter->get_second();
            auto it = mapped.begin();
            //for (auto it = mapped.begin(); it.ok(); ++it) {
                //if (!it->visited) {
                if(!mapped.is_visited()){
                    block_size++;
                    for (size_t j = 0; j < left_col_len; ++j) {
                        auto& column = *it->block->get_by_position(j).column;
                        for (int jj = 0; jj < (it->block->columns()); ++jj)
                            LOG(INFO) << "hashtable matched probe_data: "<< (it->block->get_by_position(jj)).to_string(it->row_num);
                        mcol[j]->insert_from(column, it->row_num);
                    }
                }
            //}
        }
        // select citycode from table6 except select citycode from table5;
        // select citycode from table6 except select citycode from table7;
        // select username from table6 except select username from table7;
        // select username from table5 except select username from table6;
        // select username from table6 except select username from table5;
        // select *  from table6 except select *  from table7;
        // select *  from table5 except select *  from table6;
        // select *  from table6 except select *  from table5;
        // select k7 from test_mysql_vec except select k7 from test_mysql_vec2; 
        // select k4 from test_mysql_vec except select k4 from test_mysql_vec2; 
        // select k5 from test_mysql_vec except select k5 from test_mysql_vec2;
        // select k11 from test_mysql_vec except select k11 from test_mysql_vec2;


        // select citycode from table6 intersect select citycode from table5;
        // select citycode from table6 intersect select citycode from table7;
        // select username from table6 intersect select username from table7;
        // select username from table5 intersect select username from table6;
        // select username from table6 intersect select username from table5;
        // select *  from table6 intersect select *  from table7;
        // select *  from table5 intersect select *  from table6;
        // select *  from table6 intersect select *  from table5;    
        // select k7 from test_mysql_vec intersect select k7 from test_mysql_vec2; 
        // select k4 from test_mysql_vec intersect select k4 from test_mysql_vec2; 
        // select k5 from test_mysql_vec intersect select k5 from test_mysql_vec2;
        // select k11 from test_mysql_vec intersect select k11 from test_mysql_vec2;        

        *eos = iter == hash_table_ctx.hash_table.end();

        output_block->swap(mutable_block.to_block());

        int64_t m = output_block->rows();
        COUNTER_UPDATE(_rows_returned_counter, m);
        _num_rows_returned += m;
        LOG(INFO)<<"output_block_rows: "<<m;
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
    ColumnRawPtrs& _probe_raw_ptrs; //using ColumnRawPtrs = std::vector<const IColumn*>;
    Arena& _arena;
    RuntimeProfile::Counter* _rows_returned_counter;
};

VExceptNode::VExceptNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VSetOperationNode(pool, tnode, descs) {}

Status VExceptNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::init(tnode, state));
    DCHECK(tnode.__isset.except_node);

    for (int i = 0; i < _child_expr_lists.size(); ++i) {
        LOG(INFO)<<"debug string: "<<doris::vectorized::VExpr::debug_string(_child_expr_lists[i]);
        LOG(INFO) << "i: " << i;
        for (int j = 0; j < _child_expr_lists[i].size(); ++j) 
            LOG(INFO) << "j: " << j;
    }
    return Status::OK();
}

Status VExceptNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VSetOperationNode::prepare(state));
    _probe_timer = ADD_TIMER(runtime_profile(), "ProbeTime");
    _right_table_data_types = VectorizedUtils::get_data_types(child(1)->row_desc());
    _left_table_data_types = VectorizedUtils::get_data_types(child(0)->row_desc());

    LOG(INFO)<<"_right_table_data_types: size: "<<_right_table_data_types.size()<<" _left_table_data_types: "<<_left_table_data_types.size();
    for(int i=0;i<_right_table_data_types.size();++i)
        LOG(INFO)<<"right: "<<_right_table_data_types[i]->get_name()<<" left: "<<_left_table_data_types[i]->get_name();
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
    if ((probe_rows == 0 || _probe_index == probe_rows)&& !_probe_eos ) 
    {
        _probe_index = 0;
        _probe_block.clear();
        int i=0;
        do {
            RETURN_IF_ERROR(child(1)->get_next(state, &_probe_block, &_probe_eos));
            LOG(INFO)<<i++<<" probe: "<<_probe_block.columns()<<" rows: "<<_probe_block.rows()<<" ------------------------------";
            for(int k=0;k<_probe_block.rows();++k)
            {
                for(int j=0;j<_probe_block.columns();++j)
                    LOG(INFO)<<"data: "<<_probe_block.get_by_position(j).to_string(k);
            }
        } while (_probe_block.rows() == 0 && !_probe_eos);

        probe_rows = _probe_block.rows();
        if(probe_rows!=0)
        {
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
                        ProcessHashTableProbe3<HashTableCtxType> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.mark_data_in_hashtable(arg);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                },
                _hash_table_variants);
    } else if(_probe_eos) {
        MutableBlock mutable_block(VectorizedUtils::create_empty_columnswithtypename(child(0)->row_desc()));
        output_block->clear();
        std::visit(
                [&](auto&& arg) {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        ProcessHashTableProbe3<HashTableCtxType> process_hashtable_ctx(
                                this, state->batch_size(), probe_rows);
                        st = process_hashtable_ctx.get_data_in_hashtable(arg, mutable_block,output_block, eos);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                },
                _hash_table_variants);
    } 
    LOG(INFO)<<"I am  to exit "<<*eos;
    return st;
}

Status VExceptNode::close(RuntimeState* state) {
    return VSetOperationNode::close(state);
}

} // namespace vectorized
} // namespace doris
