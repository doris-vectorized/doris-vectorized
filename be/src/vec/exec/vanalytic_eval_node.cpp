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

#include "vec/exec/vanalytic_eval_node.h"

#include "exprs/agg_fn_evaluator.h"
#include "exprs/anyval_util.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "udf/udf_internal.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized  {
VAnalyticEvalNode::VAnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.analytic_node.intermediate_tuple_id),
          _output_tuple_id(tnode.analytic_node.output_tuple_id),
          _window(tnode.analytic_node.window) {
    if (tnode.analytic_node.__isset.buffered_tuple_id) {
        _buffered_tuple_id = tnode.analytic_node.buffered_tuple_id;
    }
    AnalyticFnScope _fn_scope = AnalyticFnScope::PARTITION;
    if (!tnode.analytic_node.__isset.window) {                                             //Unbounded:  [unbounded preceding,unbounded following]
        _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_unbounded, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    } else if (tnode.analytic_node.window.type == TAnalyticWindowType::RANGE) {           
        DCHECK(!_window.__isset.window_start) << "RANGE windows must have UNBOUNDED PRECEDING";
        DCHECK(!_window.__isset.window_end ||_window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW)<< "RANGE window end bound must be CURRENT ROW or UNBOUNDED FOLLOWING";
        if (!_window.__isset.window_end) {                                                
            _fn_scope = AnalyticFnScope::PARTITION;
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_unbounded, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        } else {
            _fn_scope = AnalyticFnScope::RANGE;                                //range:     [unbounded preceding,current row]
        _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_range, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
    } else { //rows
        if (_window.__isset.window_start) {
            TAnalyticWindowBoundary b = _window.window_start;
            if (b.__isset.rows_offset_value) {                              //[offset     ,   ]
                _rows_start_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_start_offset *= -1;  //preceding--> negative
                }                              //current_row  0
            } else {                           //following    positive
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);//[current row,   ]
                _rows_start_offset = 0;
            }
        }
        if (_window.__isset.window_end) {
            TAnalyticWindowBoundary b = _window.window_end;             
            if (b.__isset.rows_offset_value) {                              //[       , offset]
                _rows_end_offset = b.rows_offset_value;
                if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
                    _rows_end_offset *= -1;
                }
            } else {
                DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::CURRENT_ROW);//[   ,current row]
                _rows_end_offset = 0;
            }
        }

        if (!_window.__isset.window_start && !_window.__isset.window_end) {
            _fn_scope = AnalyticFnScope::PARTITION;
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_unbounded, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        } 
        else {
            _fn_scope = AnalyticFnScope::ROWS;
            _executor.get_next = std::bind<Status>(&VAnalyticEvalNode::_get_next_for_rows, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
    }
    VLOG_ROW << "tnode=" << apache::thrift::ThriftDebugString(tnode)<<" AnalyticFnScope: "<<debug_string();
}

Status VAnalyticEvalNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    const TAnalyticNode& analytic_node = tnode.analytic_node;
    size_t agg_size = analytic_node.analytic_functions.size();
    _agg_expr_ctxs.resize(agg_size);
    _agg_intput_columns.resize(agg_size);

    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = analytic_node.analytic_functions[i];
        const TFunction& fn = desc.nodes[0].fn;
        int node_idx = 0;
        _agg_intput_columns[i].resize(desc.nodes[0].num_children);
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            VExpr* expr = nullptr;
            VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(VExpr::create_tree_from_thrift(_pool, desc.nodes, nullptr, &node_idx,&expr, &ctx));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }
        if (fn.name.function_name == "row_number" || fn.name.function_name == "rank" || fn.name.function_name == "dense_rank") {
            _executor.execute = std::bind<void>(&VAnalyticEvalNode::_execute_for_rank, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
        } else if (fn.name.function_name == "lead" || fn.name.function_name == "lag") {
            _executor.execute = std::bind<void>(&VAnalyticEvalNode::_execute_for_lead, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);       
        } else {
            _executor.execute = std::bind<void>(&VAnalyticEvalNode::_execute_for_agg, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
        }

        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, analytic_node.analytic_functions[i], &evaluator));
        _agg_functions.emplace_back(evaluator);
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            _agg_intput_columns[i][j] = _agg_expr_ctxs[i][j]->root()->data_type()->create_column();
        }
    }

    _agg_functions_size = _agg_functions.size();
    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, analytic_node.partition_exprs, &_partition_by_eq_expr_ctxs));
    RETURN_IF_ERROR(VExpr::create_expr_trees(_pool, analytic_node.order_by_exprs, &_order_by_eq_expr_ctxs));
    _partition_by_columns.resize(_partition_by_eq_expr_ctxs.size());
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size(); ++i) {
        _partition_by_columns[i] =_partition_by_eq_expr_ctxs[i]->root()->data_type()->create_column();
    }
    _order_by_columns.resize(_order_by_eq_expr_ctxs.size());
    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        _order_by_columns[i] =_order_by_eq_expr_ctxs[i]->root()->data_type()->create_column();
    }

    return Status::OK();
}

Status VAnalyticEvalNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK(child(0)->row_desc().is_prefix_of(row_desc()));
    _mem_pool.reset(new MemPool(mem_tracker().get()));
    _evaluation_timer = ADD_TIMER(runtime_profile(), "EvaluationTime");
    SCOPED_TIMER(_evaluation_timer);

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[i];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[i];
        RETURN_IF_ERROR(_agg_functions[i]->prepare(state, child(0)->row_desc(), _mem_pool.get(),intermediate_slot_desc, output_slot_desc, mem_tracker()));
    }

    _offsets_of_aggregate_states.resize(_agg_functions_size);
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;
        const auto& agg_function = _agg_functions[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();
        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _agg_functions_size) {
            size_t alignment_of_next_state = _agg_functions[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError(fmt::format("Logical error: align_of_data is not 2^N"));
            }
            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states = (_total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }
    }
    _fn_place_ptr = _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states,_align_aggregate_states);
    _create_agg_status(_fn_place_ptr);
    _executor.insert_result = std::bind<void>(&VAnalyticEvalNode::_insert_result,this, std::placeholders::_1, std::placeholders::_2);

    for (const auto& ctx : _agg_expr_ctxs) {
        VExpr::prepare(ctx, state, child(0)->row_desc(), expr_mem_tracker());
    }
    if (!_partition_by_eq_expr_ctxs.empty() || !_order_by_eq_expr_ctxs.empty()) {
        vector<TTupleId> tuple_ids;
        tuple_ids.push_back(child(0)->row_desc().tuple_descriptors()[0]->id());
        tuple_ids.push_back(_buffered_tuple_id);
        RowDescriptor cmp_row_desc(state->desc_tbl(), tuple_ids, vector<bool>(2, false));
        if (!_partition_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_partition_by_eq_expr_ctxs, state, cmp_row_desc, expr_mem_tracker()));
        }
        if (!_order_by_eq_expr_ctxs.empty()) {
            RETURN_IF_ERROR(VExpr::prepare(_order_by_eq_expr_ctxs, state, cmp_row_desc, expr_mem_tracker()));
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    RETURN_IF_ERROR(VExpr::open(_partition_by_eq_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_order_by_eq_expr_ctxs, state));
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        RETURN_IF_ERROR(VExpr::open(_agg_expr_ctxs[i], state));
    }
    return Status::OK();
}

Status VAnalyticEvalNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    ExecNode::close(state);
    _destory_agg_status(_fn_place_ptr);
    return Status::OK();
}

Status VAnalyticEvalNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VAnalyticEvalNode::get_next.");
}

Status VAnalyticEvalNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    if (reached_limit() || (_input_eos && _output_block_index == _input_blocks.size())) {
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_executor.get_next(state, block, eos));
    if (*eos) {
        return Status::OK();
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_unbounded(RuntimeState* state, Block* block, bool* eos) {
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_total_rows == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_evaluation_timer);
        bool next_partition = _init_next_partition(found_partition_end);
        size_t block_rows = _input_blocks[_output_block_index].rows();
        RETURN_IF_ERROR(_init_result_columns());
        if (next_partition) {
            _executor.execute(_partition_start, _partition_end, _partition_start, _partition_end);
        }

        int64_t first_block_row_position = input_block_first_row_positions[_output_block_index];
        int64_t get_value_start = _current_row_position - first_block_row_position;
        int64_t get_value_end = std::min<int64_t>(_current_row_position + block_rows, _partition_end);
        _window_result_position = std::min<int64_t>(get_value_end - first_block_row_position, block_rows); 
        _executor.insert_result(get_value_start, _window_result_position);  
        _current_row_position += (_window_result_position - get_value_start);  

        if (_window_result_position == block_rows) {
            return _output_result_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_range(RuntimeState* state, Block* block, bool* eos){
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_total_rows == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_evaluation_timer);
        _init_next_partition(found_partition_end);
        size_t block_rows = _input_blocks[_output_block_index].rows();
        RETURN_IF_ERROR(_init_result_columns());

        while (_current_row_position < _partition_end && _window_result_position < block_rows) {
            if (_current_row_position >= _peer_group_end) {
                _get_peer_group_end();
                DCHECK_GE(_peer_group_end, _peer_group_start);
                _executor.execute(_peer_group_start, _peer_group_end, _peer_group_start, _peer_group_end); 
            }
            int64_t first_block_row_position = input_block_first_row_positions[_output_block_index];
            int64_t get_value_start = _current_row_position - first_block_row_position;
            _window_result_position = std::min<int64_t>(_peer_group_end - first_block_row_position, block_rows);
            _executor.insert_result(get_value_start, _window_result_position);  
            _current_row_position += (_window_result_position - get_value_start);
        }

        if (_window_result_position == block_rows) {
            return _output_result_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_get_next_for_rows(RuntimeState* state, Block* block, bool* eos) {
    while (!_input_eos || _output_block_index < _input_blocks.size()) {
        int64_t found_partition_end = 0;
        RETURN_IF_ERROR(_try_fetch_next_partition_data(state, &found_partition_end));
        if (_input_eos && _input_total_rows == 0) {
            *eos = true;
            return Status::OK();
        }
        SCOPED_TIMER(_evaluation_timer);
        _init_next_partition(found_partition_end);
        size_t block_rows = _input_blocks[_output_block_index].rows();
        RETURN_IF_ERROR(_init_result_columns());

        while (_current_row_position < _partition_end && _window_result_position < block_rows) {
            int64_t range_start = 0;
            int64_t range_end = 0;
            if(!_window.__isset.window_start && _window.window_end.type == TAnalyticWindowBoundaryType::CURRENT_ROW) { //[preceding, current_row],[current_row, following]
                range_start = _current_row_position;
                range_end = _current_row_position + 1;  //going on calculate,add up data, no need to reset state
            } else {
                _reset_agg_status();
                if (!_window.__isset.window_start) { //[preceding, offset]        --unbound: [preceding, following]
                    range_start = _partition_start;
                } else {
                    range_start = _current_row_position + _rows_start_offset;
                }
                range_end = _current_row_position + _rows_end_offset + 1;
            }
            _executor.execute(_partition_start, _partition_end, range_start, range_end);
            _window_result_position++;
            int64_t get_value_start = _current_row_position - input_block_first_row_positions[_output_block_index];
            _executor.insert_result(get_value_start, _window_result_position);  
            _current_row_position++; 
        }

        if (_window_result_position == block_rows) {
            return _output_result_block(block);
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end) {
    *partition_end = _get_partition_end();          
    while (_need_fetch_next_block(*partition_end)) { 
        RETURN_IF_ERROR(_fetch_next_block(state));   
        *partition_end = _get_partition_end();      
    }    
    return Status::OK();
}

int64_t VAnalyticEvalNode::_get_partition_end() {
    SCOPED_TIMER(_evaluation_timer);    
    if (_current_row_position < _partition_end) {
        return _partition_end;
    }

    if (_partition_by_eq_expr_ctxs.empty() || (_input_total_rows == 0)) { 
        return _input_total_rows;
    }

    int64_t found_partition_end = _partition_by_columns[0]->size();
    for (size_t i = 0; i < _partition_by_columns.size(); ++i) {
        found_partition_end = _compare_row(_partition_by_columns[i].get(), _partition_end, found_partition_end);
    }
    return found_partition_end;
}

int64_t VAnalyticEvalNode::_compare_row(IColumn* column, int64_t start, int64_t end) {
    int64_t target = start;
    while (start < end) {
        int64_t mid = (start + end) >> 1;
        if (column->compare_at(target, mid, *column, 1))
            end = mid;
        else
            start = mid + 1;
    }
    return start;
}

bool VAnalyticEvalNode::_need_fetch_next_block(int64_t found_partition_end) {  
    if (_input_eos || (_current_row_position < _partition_end)) {
        return false;
    }
    if ((_partition_by_eq_expr_ctxs.empty() && !_input_eos) || (found_partition_end == 0)) {
        return true;
    }
    if (!_partition_by_eq_expr_ctxs.empty() && found_partition_end == _partition_by_columns[0]->size() && !_input_eos) {
        return true;
    }
    return false;
}

Status VAnalyticEvalNode::_fetch_next_block(RuntimeState* state) {
    Block block;   
    RETURN_IF_CANCELLED(state);
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, &block, &_input_eos));
    } while (!_input_eos && block.rows() == 0);

    if (_input_eos && block.rows() == 0) {
        return Status::OK();
    }

    input_block_first_row_positions.emplace_back(_input_total_rows);
    size_t block_rows = block.rows();
    _input_total_rows += block_rows;
    if (_origin_cols.empty()) {
        for (int c = 0; c < block.columns(); ++c) {
            _origin_cols.emplace_back(c);
        }
    }

    for (size_t i = 0; i < _agg_functions_size; ++i) {
        for (size_t j = 0; j < _agg_expr_ctxs[i].size(); ++j) {
            RETURN_IF_ERROR(_insert_range_column(&block, _agg_expr_ctxs[i][j], _agg_intput_columns[i][j].get(), block_rows));
        }
    }
    for (size_t i = 0; i < _partition_by_eq_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(_insert_range_column(&block, _partition_by_eq_expr_ctxs[i], _partition_by_columns[i].get(), block_rows));
    }
    for (size_t i = 0; i < _order_by_eq_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(_insert_range_column(&block, _order_by_eq_expr_ctxs[i], _order_by_columns[i].get(), block_rows));
    }
    _input_blocks.emplace_back(std::move(block));
    return Status::OK();
}

Status VAnalyticEvalNode::_insert_range_column(vectorized::Block* block, VExprContext* expr, IColumn* dst_column,size_t length) {
    int result_col_id = -1;
    RETURN_IF_ERROR(expr->execute(block, &result_col_id));
    DCHECK_GE(result_col_id, 0);
    auto column = block->get_by_position(result_col_id).column->convert_to_full_column_if_const();
    dst_column->insert_range_from(*column, 0, length);
    return Status::OK();
}

bool VAnalyticEvalNode::_init_next_partition(int64_t found_partition_end) {
    if ((_current_row_position >= _partition_end) && ((_partition_end == 0) || (_partition_end != found_partition_end))) {
        _partition_start = _partition_end;  
        _partition_end = found_partition_end;         
        _current_row_position = _partition_start; 
        _reset_agg_status();
        return true;
    }
    return false;
}

void VAnalyticEvalNode::_insert_result(int32_t start, int32_t end) {
    for (int i = 0; i < _agg_functions_size; ++i) {
        for (int j = start; j < end; ++j) {
            _agg_functions[i]->insert_result_info(_fn_place_ptr + _offsets_of_aggregate_states[i], _result_window_columns[i].get());
        }
    }
}

Status VAnalyticEvalNode::_output_result_block(Block* block) {
    Block output_block = std::move(_input_blocks[_output_block_index]);
    if (_origin_cols.size() < output_block.columns()) {
        output_block.erase_not_in(_origin_cols);
    }

    for (size_t i = 0; i < _result_window_columns.size(); ++i) {
        output_block.insert({std::move(_result_window_columns[i]),_agg_functions[i]->data_type(),""});
    }

    _num_rows_returned += output_block.rows();
    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        output_block.set_num_rows(output_block.rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        *block = output_block;
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    *block = output_block;
    _output_block_index++;
    _window_result_position = 0;
    return Status::OK();
}

void VAnalyticEvalNode::_execute_for_rank(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end) {
    for(size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->function()->add_range_single_place(peer_group_start,peer_group_end, _fn_place_ptr + _offsets_of_aggregate_states[i], nullptr, nullptr);
    }
}

void VAnalyticEvalNode::_execute_for_lead(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end) {
    for(size_t i = 0; i < _agg_functions_size; ++i) {
        std::vector<const IColumn*> _agg_columns;
        for (int j = 0; j < _agg_intput_columns[i].size(); ++j) {
            _agg_columns.push_back(_agg_intput_columns[i][j].get());
        }
        _agg_functions[i]->function()->add_range_single_place(frame_start,frame_end, _fn_place_ptr + _offsets_of_aggregate_states[i], _agg_columns.data(), nullptr, peer_group_end);                                
    }
}

void VAnalyticEvalNode::_execute_for_agg(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end) {
    for(size_t i = 0; i < _agg_functions_size; ++i) {
        frame_start = std::max<int64_t>(frame_start, _partition_start); 
        frame_end = std::min<int64_t>(frame_end, _partition_end);       //update peer_group or range pos could overstep partition area
        if(frame_start > frame_end) return;                             //maybe have set range start=0, but end bound preceding is negative
        const IColumn* agg_column = _agg_intput_columns[i][0].get();
        _agg_functions[i]->function()->add_range_single_place(frame_start,frame_end, _fn_place_ptr + _offsets_of_aggregate_states[i], &agg_column, nullptr);
    }
}

void VAnalyticEvalNode::_get_peer_group_end() {
    if (_current_row_position < _peer_group_end) {
        return;
    }
    _peer_group_start = _peer_group_end;
    _peer_group_end = _partition_end;
    for (size_t i = 0; i < _order_by_columns.size(); ++i) {
        _peer_group_end = _compare_row(_order_by_columns[i].get(), _peer_group_start, _peer_group_end);
    }
}

Status VAnalyticEvalNode::_init_result_columns() {
    if (!_window_result_position) {  
        _result_window_columns.resize(_agg_functions_size);
        for (size_t i = 0; i < _agg_functions_size; ++i) {
            _result_window_columns[i] = _agg_functions[i]->data_type()->create_column();//return type
        }
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_reset_agg_status() {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->reset(_fn_place_ptr + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_create_agg_status(AggregateDataPtr data) {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->create(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status VAnalyticEvalNode::_destory_agg_status(AggregateDataPtr data) {
    for (size_t i = 0; i < _agg_functions_size; ++i) {
        _agg_functions[i]->destroy(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

std::string VAnalyticEvalNode::debug_string() {
    std::stringstream ss;
    if (_fn_scope == PARTITION) {
        ss << "NO WINDOW";
        return ss.str();
    }
    ss << "{type=";
    if (_fn_scope == RANGE) {
        ss << "RANGE";
    } else {
        ss << "ROWS";
    }
    ss << ", start=";
    if (_window.__isset.window_start) {
        TAnalyticWindowBoundary start = _window.window_start;
        ss << debug_window_bound_string(start);
    } else {
        ss << "UNBOUNDED_PRECEDING";
    }
    ss << ", end=";
    if (_window.__isset.window_end) {
        TAnalyticWindowBoundary end = _window.window_end;
        ss << debug_window_bound_string(end) << "}";
    } else {
        ss << "UNBOUNDED_FOLLOWING";
    }
    return ss.str();
}

std::string VAnalyticEvalNode::debug_window_bound_string(TAnalyticWindowBoundary b) {
    if (b.type == TAnalyticWindowBoundaryType::CURRENT_ROW) {
        return "CURRENT_ROW";
    }
    std::stringstream ss;
    if (b.__isset.rows_offset_value) {
        ss << b.rows_offset_value;
    } else {
        DCHECK(false) << "Range offsets not yet implemented";
    }
    if (b.type == TAnalyticWindowBoundaryType::PRECEDING) {
        ss << " PRECEDING";
    } else {
        DCHECK_EQ(b.type, TAnalyticWindowBoundaryType::FOLLOWING);
        ss << " FOLLOWING";
    }
    return ss.str();
}

} // namespace doris






