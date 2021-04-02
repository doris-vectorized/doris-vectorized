#pragma once
#include "runtime/types.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
namespace vectorized {
class AggFnEvaluator {
public:
    static Status create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result);

    Status prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                   const SlotDescriptor* intermediate_slot_desc,
                   const SlotDescriptor* output_slot_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    // create/destroy AGG Data
    void create(AggregateDataPtr place);
    void destroy(AggregateDataPtr place);

    // agg_function
    // void execute_single_add(int row_size, AggregateDataPtr place);
    void execute_single_add(Block* block, AggregateDataPtr place);

    void insert_result_info(AggregateDataPtr place ,IColumn *column);

    // void execute_add(int row_size, AggregateDataPtr* places, size_t place_offset);

    DataTypePtr& data_type() { return _data_type; }

private:
    const TFunction _fn;

    AggFnEvaluator(const TExprNode& desc);

    const TypeDescriptor _return_type;
    const TypeDescriptor _intermediate_type;

    const SlotDescriptor* _intermediate_slot_desc;
    const SlotDescriptor* _output_slot_desc;

    // input context
    std::vector<VExprContext*> _input_exprs_ctxs;

    DataTypePtr _data_type;

    AggregateFunctionPtr _function;

    std::string _expr_name;
};
} // namespace vectorized

} // namespace doris
