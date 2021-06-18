#include "vec/common/arithmetic_overflow.h"
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A, typename B>
struct MinusImpl {
    using ResultType = typename NumberTraits::ResultOfSubtraction<A, B>::Type;
    static const constexpr bool allow_decimal = true;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) - b;
    }

    /// Apply operation and check overflow. It's used for Deciamal operations. @returns true if overflowed, false otherwise.
    template <typename Result = ResultType>
    static inline bool apply(A a, B b, Result& c) {
        return common::subOverflow(static_cast<Result>(a), b, c);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        return left->getType()->isIntegerTy() ? b.CreateSub(left, right)
                                              : b.CreateFSub(left, right);
    }
#endif
};

struct NameMinus {
    static constexpr auto name = "subtract";
};
using FunctionMinus = FunctionBinaryArithmetic<MinusImpl, NameMinus>;

//void registerFunctionMinus(FunctionFactory & factory)
//{
//    factory.registerFunction<FunctionMinus>();
//}
void registerFunctionMinus(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionMinus>();
}
} // namespace doris::vectorized
