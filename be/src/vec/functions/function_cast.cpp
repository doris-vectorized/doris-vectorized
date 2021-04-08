#include "vec/functions/function_cast.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
void registerFunctionCast(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionBuilderCast>();
}
} // namespace doris::vectorized
