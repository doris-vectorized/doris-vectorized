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
#include "fmt/format.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// support string->complex/primary
// support primary/complex->primary/complex
// support primary -> string
template <typename Impl, typename Name>
class FunctionUnaryToType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionUnaryToType>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::ReturnType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) override {
        return _executeImpl<typename Impl::ReturnType>(block, arguments, result, input_rows_count);
    }

private:
    // handle result == DataTypeString
    template <typename T, std::enable_if_t<std::is_same_v<T, DataTypeString>, T>* = nullptr>
    Status _executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if constexpr (std::is_integer(Impl::TYPE_INDEX)) {
            if (auto* col = checkAndGetColumn<ColumnVector<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(
                        Impl::vector(col->getData(), col_res->getChars(), col_res->getOffsets()));
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        }

        return Status::RuntimeError(fmt::format("Illegal column {} of argument of function {}",
                                                block.getByPosition(arguments[0]).column->getName(),
                                                getName()));
    }
    template <typename T, std::enable_if_t<!std::is_same_v<T, DataTypeString>, T>* = nullptr>
    Status _executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if constexpr (Impl::TYPE_INDEX == TypeIndex::String) {
            if (const ColumnString* col = checkAndGetColumn<ColumnString>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(
                        Impl::vector(col->getChars(), col->getOffsets(), col_res->getData()));
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        } else if constexpr (std::is_integer(Impl::TYPE_INDEX)) {
            if (const auto* col =
                        checkAndGetColumn<ColumnVector<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->getData(), col_res->getData()));
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        } else if constexpr (is_complex_v<typename Impl::Type>) {
            if (const auto* col =
                        checkAndGetColumn<ColumnComplexType<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->getData(), col_res->getData()));
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("Illegal column {} of argument of function {}",
                                                block.getByPosition(arguments[0]).column->getName(),
                                                getName()));
    }
};

template <typename LeftDataType, typename RightDataType,
          template <typename, typename> typename Impl, typename Name>
class FunctionBinaryToType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryToType>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;
        return std::make_shared<ResultDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t /*input_rows_count*/) override {
        DCHECK_EQ(arguments.size(), 2);
        const auto& left = block.getByPosition(arguments[0]);
        auto lcol = left.column->convertToFullColumnIfConst();
        const auto& right = block.getByPosition(arguments[1]);
        auto rcol = right.column->convertToFullColumnIfConst();

        using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;

        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        using ColVecLeft =
                std::conditional_t<is_complex_v<T0>, ColumnComplexType<T0>, ColumnVector<T0>>;
        using ColVecRight =
                std::conditional_t<is_complex_v<T1>, ColumnComplexType<T1>, ColumnVector<T1>>;

        using ColVecResult =
                std::conditional_t<is_complex_v<ResultType>, ColumnComplexType<ResultType>,
                                   ColumnVector<ResultType>>;

        typename ColVecResult::MutablePtr col_res = nullptr;

        col_res = ColVecResult::create();
        auto& vec_res = col_res->getData();
        vec_res.resize(block.rows());

        if (auto col_left = checkAndGetColumn<ColVecLeft>(lcol.get())) {
            if (auto col_right = checkAndGetColumn<ColVecRight>(rcol.get())) {
                Impl<LeftDataType, RightDataType>::vector_vector(col_left->getData(),
                                                                 col_right->getData(), vec_res);
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", getName()));
    }
};

template <template <typename, typename> typename Impl, typename Name>
class FunctionBinaryToType<DataTypeString, DataTypeString, Impl, Name> : public IFunction {
public:
    using LeftDataType = DataTypeString;
    using RightDataType = DataTypeString;
    using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;

    using ColVecLeft = ColumnString;
    using ColVecRight = ColumnString;

    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryToType>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        return std::make_shared<ResultDataType>();
    }
    bool useDefaultImplementationForConstants() const override { return true; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t /*input_rows_count*/) override {
        const auto& left = block.getByPosition(arguments[0]);
        const auto& right = block.getByPosition(arguments[1]);
        return execute_inner_impl<ResultDataType>(left, right, block, arguments, result);
    }

private:
    template <typename ReturnDataType,
              std::enable_if_t<!std::is_same_v<ResultDataType, DataTypeString>, ReturnDataType>* =
                      nullptr>
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) {
        auto lcol = left.column->convertToFullColumnIfConst();
        auto rcol = right.column->convertToFullColumnIfConst();

        using ResultType = typename ResultDataType::FieldType;
        using ColVecResult = ColumnVector<ResultType>;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();

        auto& vec_res = col_res->getData();
        vec_res.resize(block.rows());

        if (auto col_left = checkAndGetColumn<ColVecLeft>(lcol.get())) {
            if (auto col_right = checkAndGetColumn<ColVecRight>(rcol.get())) {
                Impl<LeftDataType, RightDataType>::vector_vector(
                        col_left->getChars(), col_left->getOffsets(), col_right->getChars(),
                        col_right->getOffsets(), vec_res);
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", getName()));
    }

    template <typename ReturnDataType,
              std::enable_if_t<std::is_same_v<ResultDataType, DataTypeString>, ReturnDataType>* =
                      nullptr>
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) {
        auto lcol = left.column->convertToFullColumnIfConst();
        auto rcol = right.column->convertToFullColumnIfConst();

        using ColVecResult = ColumnString;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();
        if (auto col_left = checkAndGetColumn<ColVecLeft>(lcol.get())) {
            if (auto col_right = checkAndGetColumn<ColVecRight>(rcol.get())) {
                Impl<LeftDataType, RightDataType>::vector_vector(
                        col_left->getChars(), col_left->getOffsets(), col_right->getChars(),
                        col_right->getOffsets(), col_res->getChars(), col_res->getOffsets());
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", getName()));
    }
};

// func(string,string) -> nullable(type)
template <typename Impl>
class FunctionBinaryStringOperateToNullType : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() {
        return std::make_shared<FunctionBinaryStringOperateToNullType>();
    }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        return makeNullable(std::make_shared<typename Impl::ReturnType>());
    }
    bool useDefaultImplementationForConstants() const override { return true; }
    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[2];

        // focus convert const to full column to simply execute logic
        // handle
        for (int i = 0; i < 2; ++i) {
            argument_columns[i] =
                    block.getByPosition(arguments[i]).column->convertToFullColumnIfConst();
            if (auto* nullable = checkAndGetColumn<ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->getNestedColumnPtr();
                VectorizedUtils::update_null_map(null_map->getData(), nullable->getNullMapData());
            }
        }

        auto res = Impl::ColumnType::create();

        auto specific_str_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto specific_char_column = assert_cast<const ColumnString*>(argument_columns[1].get());

        auto& ldata = specific_str_column->getChars();
        auto& loffsets = specific_str_column->getOffsets();

        auto& rdata = specific_char_column->getChars();
        auto& roffsets = specific_char_column->getOffsets();

        // execute Impl
        if constexpr (std::is_same_v<typename Impl::ReturnType, DataTypeString>) {
            auto& res_data = res->getChars();
            auto& res_offsets = res->getOffsets();
            Impl::vector_vector(ldata, loffsets, rdata, roffsets, res_data, res_offsets,
                                null_map->getData());
        } else {
            Impl::vector_vector(ldata, loffsets, rdata, roffsets, res->getData(),
                                null_map->getData());
        }

        block.getByPosition(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};
} // namespace doris::vectorized
