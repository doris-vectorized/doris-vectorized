#include "vec/common/arithmetic_overflow.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct PlusImpl {
    using ResultType = typename NumberTraits::ResultOfAdditionMultiplication<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        /// Next everywhere, static_cast - so that there is no wrong result in expressions of the form Int64 c = UInt32(a) * Int32(-1).
        return static_cast<Result>(a) + b;
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result& c) {
        return common::add_overflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        return left->getType()->isIntegerTy() ? b.CreateAdd(left, right)
                                              : b.CreateFAdd(left, right);
    }
#endif
};

struct NamePlus {
    static constexpr auto name = "add";
};
using FunctionPlus = FunctionBinaryArithmetic<PlusImpl, NamePlus>;

//void registerFunctionPlus(FunctionFactory & factory)
//{
//    factory.registerFunction<FunctionPlus>();
//}
void registerFunctionPlus(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionPlus>();
}
} // namespace doris::vectorized
