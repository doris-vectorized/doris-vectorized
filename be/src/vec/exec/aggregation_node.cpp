#include "vec/exec/aggregation_node.h"

#include "runtime/row_batch.h"
#include "exec/exec_node.h"
#include "runtime/mem_pool.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(NULL),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(NULL),
          _needs_finalize(tnode.agg_node.need_finalize),
          _single_data_ptr(nullptr) {}

AggregationNode::~AggregationNode() {}

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, tnode.agg_node.aggregate_functions[i], &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
        SlotDescriptor* desc = _intermediate_tuple_desc->slots()[i];
        VExpr* expr = new VSlotRef(desc);
        state->obj_pool()->add(expr);
        _build_expr_ctxs.push_back(new VExprContext(expr));
        state->obj_pool()->add(_build_expr_ctxs.back());
    }

    RowDescriptor build_row_desc(_intermediate_tuple_desc, false);
    RETURN_IF_ERROR(VExpr::prepare(_build_expr_ctxs, state, build_row_desc, expr_mem_tracker()));
    _mem_pool.reset(new MemPool(mem_tracker().get()));

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
                                                          _mem_pool.get(), intermediate_slot_desc,
                                                          output_slot_desc, mem_tracker()));
    }

    if (_probe_expr_ctxs.empty()) {
        const auto& slots = _intermediate_tuple_desc->slots();
        // tmp code for test
        // TODO:
        _single_data_ptr = new char[100];
        _single_data_offset = {0, 8, 16, 24, 32};

        ColumnsWithTypeAndName intermediate;
        intermediate.reserve(slots.size());

        for (const auto slot : slots) {
            auto data_type_ptr = slot->get_data_type_ptr();
            intermediate.emplace_back(data_type_ptr->createColumn(), data_type_ptr,
                                      fmt::format("slot-id:{}", slot->id()));
        }

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->create(_single_data_ptr + _single_data_offset[i]);
        }
        _single_output_block.reset(new Block(std::move(intermediate)));
    }
    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(VExecNode::open(state));
    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_build_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }

    RETURN_IF_ERROR(_children[0]->open(state));
    // TODO: get child(0) data
    bool eos = false;
    Block block;
    RowBatch batch(child(0)->row_desc(), state->batch_size(), mem_tracker().get());
    while (!eos) {
        batch.clear();
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next(state, &batch, &eos));
        block = batch.convert_to_vec_block();
        // RETURN_IF_ERROR(static_cast<VExecNode*>(_children[0])->get_next(state, &block, &eos));
        // process no grouping
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->execute_single_add(&block,
                                                         _single_data_ptr + _single_data_offset[i]);
        }
    }

    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    throw Exception("Not Implemented Aggregation Node::get_next scalar",
                    ErrorCodes::NOT_IMPLEMENTED);
}

Status AggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    block->clear();
    // Block tmp(VectorizedUtils::create_empty_columnswithtypename(row_desc()));
    // block->swap(tmp);
    // procsess no group by
    if (_single_output_block) {
        *block = _single_output_block->cloneEmpty();
        auto columns = _single_output_block->mutateColumns();
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            auto column = columns[i].get();
            _aggregate_evaluators[i]->insert_result_info(_single_data_ptr + _single_data_offset[i], column);
        }
        block->setColumns(std::move(columns));
        
        *eos = true;
    }

    return Status::OK();
}

Status AggregationNode::close(RuntimeState* state) {
    return Status::OK();
}
} // namespace doris::vectorized
