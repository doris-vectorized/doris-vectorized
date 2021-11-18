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
#include "exprs/expr.h"
#include "runtime/tuple.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/core/block.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/common/arena.h"
namespace doris::vectorized {

class AggFnEvaluator;
class VAnalyticEvalNode : public ExecNode {
public:
    ~VAnalyticEvalNode() {}
    VAnalyticEvalNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);
    virtual Status close(RuntimeState* state);
protected:
    virtual std::string debug_string();
private:
    
    Status _get_next_for_unbounded(RuntimeState* state, Block* block, bool* eos);
    Status _get_next_for_range(RuntimeState* state, Block* block, bool* eos);
    Status _get_next_for_rows(RuntimeState* state, Block* block, bool* eos);

    void _insert_result(int32_t start, int32_t end);
    Status _output_result_block(Block* block);
    void _get_peer_group_end();
    int64_t _compare_row(IColumn* column, int64_t start, int64_t end);
    Status _insert_range_column(vectorized::Block* block, VExprContext* expr, IColumn* dst_column,size_t length);
    Status _init_result_columns(); 
    Status _create_agg_status(AggregateDataPtr data);
    Status _destory_agg_status(AggregateDataPtr data);
    Status _reset_agg_status();
    bool _init_next_partition(int64_t found_partition_end);
    Status _try_fetch_next_partition_data(RuntimeState* state, int64_t* partition_end);
    int64_t _get_partition_end();
    bool _need_fetch_next_block(int64_t found_partition_end);
    Status _fetch_next_block(RuntimeState* state);

    void _execute_for_rank(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end);
    void _execute_for_lead(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end);
    void _execute_for_agg (int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end);

    std::string debug_window_bound_string(TAnalyticWindowBoundary b);
    using vectorized_execute = std::function<void(int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,int64_t frame_end)>;
    using vectorized_get_next = std::function<Status(RuntimeState* state, Block* block, bool* eos)>;
    using vectorized_get_result =std::function<void(int32_t start, int32_t end)>;
    using vectorized_closer = std::function<void()>;

    struct executor {
        vectorized_execute execute;
        vectorized_get_next get_next;
        vectorized_get_result insert_result;
        vectorized_closer close;
    };

    executor _executor;
private:
    enum AnalyticFnScope {
        PARTITION,
        RANGE,
        ROWS
    };
    std::vector<Block> _input_blocks; 
    std::vector<int64_t> input_block_first_row_positions;
    std::vector<AggFnEvaluator*> _agg_functions;
    std::vector<std::vector<VExprContext*>> _agg_expr_ctxs;
    std::vector<VExprContext*> _partition_by_eq_expr_ctxs;
    std::vector<VExprContext*> _order_by_eq_expr_ctxs;
    std::vector<std::vector<MutableColumnPtr>> _agg_intput_columns;
    std::vector<MutableColumnPtr> _partition_by_columns;
    std::vector<MutableColumnPtr> _order_by_columns;
    std::vector<MutableColumnPtr> _result_window_columns;

    bool _input_eos = false;
    int64_t _input_total_rows = 0;
    int64_t _output_block_index = 0;
    int64_t _window_result_position = 0;
    int64_t _current_row_position = 0;
    int64_t _partition_start = 0;
    int64_t _partition_end = 0;
    int64_t _peer_group_start = 0;
    int64_t _peer_group_end = 0;
    int64_t _rows_start_offset = 0;
    int64_t _rows_end_offset = 0;
    size_t _agg_functions_size = 0;
    std::unique_ptr<MemPool> _mem_pool;

    /// The offset of the n-th functions.
    std::vector<size_t> _offsets_of_aggregate_states;
    /// The total size of the row from the functions.
    size_t _total_size_of_aggregate_states = 0;
    /// The max align size for functions
    size_t _align_aggregate_states = 1;
    Arena _agg_arena_pool;
    AggregateDataPtr _fn_place_ptr;

    TTupleId _buffered_tuple_id = 0;
    TupleId _intermediate_tuple_id;
    TupleId _output_tuple_id;
    TAnalyticWindow _window ;
    AnalyticFnScope _fn_scope;
    TupleDescriptor* _intermediate_tuple_desc;
    TupleDescriptor* _output_tuple_desc;
    std::vector<int> _origin_cols;

    RuntimeProfile::Counter* _evaluation_timer;
};
} // namespace doris::vectorized
