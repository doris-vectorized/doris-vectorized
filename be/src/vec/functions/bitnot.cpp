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

#include "vec/data_types/number_traits.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename A>
struct BitNotImpl {
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;
    static const constexpr bool allow_fixed_string = true;
    static const constexpr bool allow_string_integer = false;

    static inline ResultType apply(A a) { return ~static_cast<ResultType>(a); }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* arg, bool) {
        if (!arg->getType()->isIntegerTy())
            throw Exception("BitNotImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateNot(arg);
    }
#endif
};

struct NameBitNot {
    static constexpr auto name = "bitnot";
};
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, false>;

void register_function_bitnot(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitNot>();
}

} // namespace doris::vectorized
