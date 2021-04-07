//#include <vec/Functions/FunctionFactory.h>
#include "vec/functions/functions_comparison.h"


namespace doris::vectorized
{

using FunctionGreater = FunctionComparison<GreaterOp, NameGreater>;
using FunctionGreaterOrEquals = FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals>;
using FunctionLess = FunctionComparison<LessOp, NameLess>;
using FunctionLessOrEquals = FunctionComparison<LessOrEqualsOp, NameLessOrEquals>;
using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;
using FunctionNotEquals = FunctionComparison<NotEqualsOp, NameNotEquals>;

//void registerFunctionGreater(FunctionFactory & factory)
//{
//    factory.registerFunction<FunctionGreater>();
//}
//
//template <>
//void FunctionComparison<GreaterOp, NameGreater>::executeTupleImpl(Block & block, size_t result, const ColumnsWithTypeAndName & x,
//                                                                  const ColumnsWithTypeAndName & y, size_t tuple_size,
//                                                                  size_t input_rows_count)
//{
//    return executeTupleLessGreaterImpl<FunctionGreater, FunctionGreater>(block, result, x, y, tuple_size, input_rows_count);
//}
}
