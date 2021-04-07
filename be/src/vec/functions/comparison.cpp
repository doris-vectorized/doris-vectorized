#include "vec/functions/functions_comparison.h"
#include "vec/functions/simple_function_factory.h"


namespace doris::vectorized
{
using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;
using FunctionLess = FunctionComparison<LessOp, NameLess>;
using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

void registerFunctionComparison(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionGreater>();
    factory.registerFunction<FunctionGreaterOrEquals>();
    factory.registerFunction<FunctionLess>();
    factory.registerFunction<FunctionLessOrEquals>();
    factory.registerFunction<FunctionEquals>();
    factory.registerFunction<FunctionNotEquals>();
}
}
