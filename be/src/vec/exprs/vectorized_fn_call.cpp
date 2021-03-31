#include "vec/exprs/vectorized_fn_call.h"
#include "fmt/format.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/data_types/data_types_number.h"
#include "vec/data_types/data_type_nullable.h"


namespace DB {
using doris::Status;

VectorizedFnCall::VectorizedFnCall(const doris::TExprNode& node) : VExpr(node) {}

doris::Status VectorizedFnCall::prepare(doris::RuntimeState* state,
                                        const doris::RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));
    _function = SimpleFunctionFactory::instance().get(_fn.name.function_name);
    if (_function == nullptr) {
        return Status::InternalError(fmt::format("Function {} is not implemented", _fn.name.function_name));
    }
    return Status::OK();
}
doris::Status VectorizedFnCall::open(doris::RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));
    return Status::OK();
}
void VectorizedFnCall::close(doris::RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

Status VectorizedFnCall::execute(DB::Block* block, int* result_column_id) {
    // for each child call execute
    DB::ColumnNumbers arguments;
    for(int i = 0;i < _children.size();++i) {
        int column_id = -1;
        _children[i]->execute(block, &column_id);
        arguments.emplace_back(column_id);
    }
    // call function
    size_t num_columns_without_result = block->columns();
    // todo spec column name
    block->insert({ nullptr, std::make_shared<DataTypeNullable>(_data_type), fmt::format("{}()",_fn.name.function_name)});
    _function->execute(*block, arguments, num_columns_without_result, block->rows(), false);
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

} // namespace DB
