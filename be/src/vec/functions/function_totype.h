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
#include "vec/functions/function.h"

namespace doris::vectorized {

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
                       size_t /*input_rows_count*/) override {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if constexpr (Impl::TYPE_INDEX == TypeIndex::String) {
            if (const ColumnString* col = checkAndGetColumn<ColumnString>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(
                        Impl::vector(col->getChars(), col->getOffsets(), col_res->getData()));
                block.getByPosition(result).column = std::move(col_res);
                return Status::OK();
            }
        } else if constexpr (is_integer(Impl::TYPE_INDEX)) {
            if (const auto* col =
                        checkAndGetColumn<ColumnVector<typename Impl::Type>>(column.get())) {
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
} // namespace doris::vectorized
