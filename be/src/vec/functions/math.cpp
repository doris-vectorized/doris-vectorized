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

#include "vec/common/field_visitors.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_math_unary.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
namespace {

struct AcosName {
    static constexpr auto name = "acos";
};
using FunctionAcos = FunctionMathUnary<UnaryFunctionVectorized<AcosName, std::acos>>;

struct AsinName {
    static constexpr auto name = "asin";
};
using FunctionAsin = FunctionMathUnary<UnaryFunctionVectorized<AsinName, std::asin>>;

struct AtanName {
    static constexpr auto name = "atan";
};
using FunctionAtan = FunctionMathUnary<UnaryFunctionVectorized<AtanName, std::atan>>;

struct CosName {
    static constexpr auto name = "cos";
};
using FunctionCos = FunctionMathUnary<UnaryFunctionVectorized<CosName, std::cos>>;

struct EImpl {
    static constexpr auto name = "e";
    static constexpr double value = 2.7182818284590452353602874713526624977572470;
};
using FunctionE = FunctionMathConstFloat64<EImpl>;

struct ExpName {
    static constexpr auto name = "exp";
};
using FunctionExp = FunctionMathUnary<UnaryFunctionVectorized<ExpName, std::exp>>;

struct LogName {
    static constexpr auto name = "log";
};
using FunctionLog = FunctionMathUnary<UnaryFunctionVectorized<LogName, std::log>>;

struct Log2Name {
    static constexpr auto name = "log2";
};
using FunctionLog2 = FunctionMathUnary<UnaryFunctionVectorized<Log2Name, std::log2>>;

struct Log10Name {
    static constexpr auto name = "log10";
};
using FunctionLog10 = FunctionMathUnary<UnaryFunctionVectorized<Log10Name, std::log10>>;

struct CeilName {
    static constexpr auto name = "ceil";
};
using FunctionCeil = FunctionMathUnary<UnaryFunctionVectorized<CeilName, std::ceil>>;

struct PiImpl {
    static constexpr auto name = "pi";
    static constexpr double value = 3.1415926535897932384626433832795028841971693;
};
using FunctionPi = FunctionMathConstFloat64<PiImpl>;

template <typename A>
struct SignImpl {
    using ResultType = Float32;
    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A> || std::is_floating_point_v<A>)
            return static_cast<ResultType>(a < A(0) ? -1 : a == A(0) ? 0 : 1);
        else if constexpr (std::is_signed_v<A>)
            return static_cast<ResultType>(a < 0 ? -1 : a == 0 ? 0 : 1);
        else if constexpr (std::is_unsigned_v<A>)
            return static_cast<ResultType>(a == 0 ? 0 : 1);
    }
};

struct NameSign {
    static constexpr auto name = "sign";
};
using FunctionSign = FunctionUnaryArithmetic<SignImpl, NameSign, false>;

template <typename A>
struct AbsImpl {
    using ResultType =
            std::conditional_t<IsDecimalNumber<A>, A, typename NumberTraits::ResultOfAbs<A>::Type>;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A>)
            return a < 0 ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a < 0 ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(std::abs(a));
    }
};

struct NameAbs {
    static constexpr auto name = "abs";
};

using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;

template <typename A>
struct NegativeImpl {
    using ResultType = A;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a) {
        if constexpr (IsDecimalNumber<A>)
            return a > 0 ? A(-a) : a;
        else if constexpr (std::is_integral_v<A> && std::is_signed_v<A>)
            return a > 0 ? static_cast<ResultType>(~a) + 1 : a;
        else if constexpr (std::is_integral_v<A> && std::is_unsigned_v<A>)
            return static_cast<ResultType>(-a);
        else if constexpr (std::is_floating_point_v<A>)
            return static_cast<ResultType>(-std::abs(a));
    }
};

struct NameNegative {
    static constexpr auto name = "negative";
};

using FunctionNegative = FunctionUnaryArithmetic<NegativeImpl, NameNegative, false>;

template <typename A>
struct PositiveImpl {
    using ResultType = A;

    static inline NO_SANITIZE_UNDEFINED ResultType apply(A a) { return static_cast<ResultType>(a); }
};

struct NamePositive {
    static constexpr auto name = "positive";
};

using FunctionPositive = FunctionUnaryArithmetic<PositiveImpl, NamePositive, false>;

struct SinName {
    static constexpr auto name = "sin";
};
using FunctionSin = FunctionMathUnary<UnaryFunctionVectorized<SinName, std::sin>>;

struct SqrtName {
    static constexpr auto name = "sqrt";
};
using FunctionSqrt = FunctionMathUnary<UnaryFunctionVectorized<SqrtName, std::sqrt>>;

struct TanName {
    static constexpr auto name = "tan";
};
using FunctionTan = FunctionMathUnary<UnaryFunctionVectorized<TanName, std::tan>>;

struct FloorName {
    static constexpr auto name = "floor";
};
using FunctionFloor = FunctionMathUnary<UnaryFunctionVectorized<FloorName, std::floor>>;

struct RoundName {
    static constexpr auto name = "round";
};
using FunctionRound = FunctionMathUnary<UnaryFunctionVectorized<RoundName, std::round>>;

} // namespace
void registerFunctionMath(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionAcos>();
    factory.registerFunction<FunctionAsin>();
    factory.registerFunction<FunctionAtan>();
    factory.registerFunction<FunctionCos>();
    factory.registerFunction<FunctionCeil>();
    factory.registerFunction<FunctionE>();
    factory.registerFunction<FunctionExp>();
    factory.registerFunction<FunctionLog>();
    factory.registerFunction<FunctionLog2>();
    factory.registerFunction<FunctionLog10>();
    factory.registerFunction<FunctionPi>();
    factory.registerFunction<FunctionSign>();
    factory.registerFunction<FunctionAbs>();
    factory.registerFunction<FunctionNegative>();
    factory.registerFunction<FunctionPositive>();
    factory.registerFunction<FunctionSin>();
    factory.registerFunction<FunctionSqrt>();
    factory.registerFunction<FunctionTan>();
    factory.registerFunction<FunctionFloor>();
    factory.registerFunction<FunctionRound>();
}
} // namespace doris::vectorized