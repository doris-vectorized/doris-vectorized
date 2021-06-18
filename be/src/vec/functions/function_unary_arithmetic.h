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

#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
//#include <vec/DataTypes/Native.h>
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
//#include <vec/Common/config.h>

//#if USE_EMBEDDED_COMPILER
//#pragma GCC diagnostic push
//#pragma GCC diagnostic ignored "-Wunused-parameter"
//#include <llvm/IR/IRBuilder.h>
//#pragma GCC diagnostic pop
//#endif

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <typename A, typename Op>
struct UnaryOperationImpl {
    using ResultType = typename Op::ResultType;
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecC = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>,
                                       ColumnVector<ResultType>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayC = typename ColVecC::Container;

    static void NO_INLINE vector(const ArrayA& a, ArrayC& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) c[i] = Op::apply(a[i]);
    }

    static void constant(A a, ResultType& c) { c = Op::apply(a); }
};

template <typename FunctionName>
struct FunctionUnaryArithmeticMonotonicity;

template <typename>
struct AbsImpl;
template <typename>
struct NegateImpl;

/// Used to indicate undefined operation
struct InvalidType;

template <template <typename> class Op, typename Name, bool is_injective>
class FunctionUnaryArithmetic : public IFunction {
    static constexpr bool allow_decimal =
            std::is_same_v<Op<Int8>, NegateImpl<Int8>> || std::is_same_v<Op<Int8>, AbsImpl<Int8>>;

    template <typename F>
    static bool castType(const IDataType* type, F&& f) {
        return castTypeToEither<
                DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeInt8,
                DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeFloat32, DataTypeFloat64,
                //                                            DataTypeDecimal<Decimal32>,
                //                                            DataTypeDecimal<Decimal64>,
                DataTypeDecimal<Decimal128>>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = Name::name;
    //    static FunctionPtr create(const Context &) { return std::make_shared<FunctionUnaryArithmetic>(); }
    static FunctionPtr create() { return std::make_shared<FunctionUnaryArithmetic>(); }

    String get_name() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isInjective(const Block&) override { return is_injective; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        DataTypePtr result;
        bool valid = castType(arguments[0].get(), [&](const auto& type) {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;

            if constexpr (IsDataTypeDecimal<DataType>) {
                if constexpr (!allow_decimal) return false;
                result = std::make_shared<DataType>(type.getPrecision(), type.get_scale());
            } else {
                result = std::make_shared<DataTypeNumber<typename Op<T0>::ResultType>>();
            }
            return true;
        });
        if (!valid)
            throw Exception("Illegal type " + arguments[0]->get_name() +
                                    " of argument of function " + get_name(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return result;
    }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t /*input_rows_count*/) override {
        bool valid = castType(block.getByPosition(arguments[0]).type.get(), [&](const auto& type) {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;

            if constexpr (IsDataTypeDecimal<DataType>) {
                if constexpr (allow_decimal) {
                    if (auto col = check_and_get_column<ColumnDecimal<T0>>(
                            block.getByPosition(arguments[0]).column.get())) {
                        auto col_res = ColumnDecimal<typename Op<T0>::ResultType>::create(
                                0, type.get_scale());
                        auto& vec_res = col_res->get_data();
                        vec_res.resize(col->get_data().size());
                        UnaryOperationImpl<T0, Op<T0>>::vector(col->get_data(), vec_res);
                        block.getByPosition(result).column = std::move(col_res);
                        return true;
                    }
                }
            } else {
                if (auto col = check_and_get_column<ColumnVector<T0>>(
                        block.getByPosition(arguments[0]).column.get())) {
                    auto col_res = ColumnVector<typename Op<T0>::ResultType>::create();
                    auto& vec_res = col_res->get_data();
                    vec_res.resize(col->get_data().size());
                    UnaryOperationImpl<T0, Op<T0>>::vector(col->get_data(), vec_res);
                    block.getByPosition(result).column = std::move(col_res);
                    return true;
                }
            }

            return false;
        });
        if (!valid)
            throw Exception(get_name() + "'s argument does not match the expected data type",
                            ErrorCodes::LOGICAL_ERROR);
        return Status::OK();
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes& arguments) const override {
        return castType(arguments[0].get(), [&](const auto& type) {
            using DataType = std::decay_t<decltype(type)>;
            return !IsDataTypeDecimal<DataType> && Op<typename DataType::FieldType>::compilable;
        });
    }

    llvm::Value* compileImpl(llvm::IRBuilderBase& builder, const DataTypes& types,
                             ValuePlaceholders values) const override {
        llvm::Value* result = nullptr;
        castType(types[0].get(), [&](const auto& type) {
            using DataType = std::decay_t<decltype(type)>;
            using T0 = typename DataType::FieldType;
            using T1 = typename Op<T0>::ResultType;
            if constexpr (!std::is_same_v<T1, InvalidType> && !IsDataTypeDecimal<DataType> &&
                          Op<T0>::compilable) {
                auto& b = static_cast<llvm::IRBuilder<>&>(builder);
                auto* v = nativeCast(b, types[0], values[0](),
                                     std::make_shared<DataTypeNumber<T1>>());
                result = Op<T0>::compile(b, v, std::is_signed_v<T1>);
                return true;
            }
            return false;
        });
        return result;
    }
#endif

    bool hasInformationAboutMonotonicity() const override {
        //        return FunctionUnaryArithmeticMonotonicity<Name>::has();
        return false;
    }

    //    Monotonicity getMonotonicityForRange(const IDataType &, const Field & left, const Field & right) const override
    //    {
    //        return FunctionUnaryArithmeticMonotonicity<Name>::get(left, right);
    //    }
};

struct PositiveMonotonicity {
    static bool has() { return true; }
    static IFunction::Monotonicity get(const Field&, const Field&) { return {true}; }
};

} // namespace doris::vectorized
