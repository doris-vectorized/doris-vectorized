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
#include "vec/functions/function_binary_arithmetic.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameBitAnd {
    static constexpr auto name = "bitand";
};

template <typename A, typename B>
struct BitAndImpl {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) & static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitAndImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateAnd(left, right);
    }
#endif
};

struct NameBitNot {
    static constexpr auto name = "bitnot";
};

template <typename A>
struct BitNotImpl {
    using ResultType = typename NumberTraits::ResultOfBitNot<A>::Type;

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

struct NameBitOr {
    static constexpr auto name = "bitor";
};

template <typename A, typename B>
struct BitOrImpl {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) | static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitOrImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateOr(left, right);
    }
#endif
};

struct NameBitXor {
    static constexpr auto name = "bitxor";
};

template <typename A, typename B>
struct BitXorImpl {
    using ResultType = typename NumberTraits::ResultOfBit<A, B>::Type;

    template <typename Result = ResultType>
    static inline Result apply(A a, B b) {
        return static_cast<Result>(a) ^ static_cast<Result>(b);
    }

#if USE_EMBEDDED_COMPILER
    static constexpr bool compilable = true;

    static inline llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* left, llvm::Value* right,
                                       bool) {
        if (!left->getType()->isIntegerTy())
            throw Exception("BitXorImpl expected an integral type", ErrorCodes::LOGICAL_ERROR);
        return b.CreateXor(left, right);
    }
#endif
};

using FunctionBitAnd = FunctionBinaryArithmetic<BitAndImpl, NameBitAnd>;
using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot, false>;
using FunctionBitOr = FunctionBinaryArithmetic<BitOrImpl, NameBitOr>;
using FunctionBitXor = FunctionBinaryArithmetic<BitXorImpl, NameBitXor>;

void register_function_bit(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitAnd>();
    factory.register_function<FunctionBitNot>();
    factory.register_function<FunctionBitOr>();
    factory.register_function<FunctionBitXor>();
}
} // namespace doris::vectorized
