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

#pragma once
//#include <Functions/FunctionFactory.h>
#include "vec/common/field_visitors.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

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

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = false; /// special type handling, some other time
#endif
};

struct NameAbs {
    static constexpr auto name = "abs";
};
using FunctionAbs = FunctionUnaryArithmetic<AbsImpl, NameAbs, false>;

//template <> struct FunctionUnaryArithmeticMonotonicity<NameAbs>
//{
//    static bool has() { return true; }
//    static IFunction::Monotonicity get(const Field & left, const Field & right)
//    {
//        Float64 left_float = left.isNull() ? -std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), left);
//        Float64 right_float = right.isNull() ? std::numeric_limits<Float64>::infinity() : applyVisitor(FieldVisitorConvertToNumber<Float64>(), right);
//
//        if ((left_float < 0 && right_float > 0) || (left_float > 0 && right_float < 0))
//            return {};
//
//        return { true, (left_float > 0) };
//    }
//};

void registerFunctionAbs(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionAbs>();
}

} // namespace doris::vectorized
