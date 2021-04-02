#pragma once
#include "exec/exec_node.h"
#include "vec/exec/exec_node.h"
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
class AggregationNode : public VExecNode {
public:
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
    std::vector<int> _single_data_offset;
    AggregateDataPtr _single_data_ptr;
    std::unique_ptr<Block> _single_output_block;
};
} // namespace vectorized
} // namespace doris
