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

#include "vec/functions/simple_function_factory.h"
#include "vec/data_types/data_type_number.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction {
public:
    static constexpr auto name = "is_not_null_pred";

    static FunctionPtr create() {
        return std::make_shared<FunctionIsNotNull>();
    }

    std::string getName() const override {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override {
        const ColumnWithTypeAndName & elem = block.getByPosition(arguments[0]);
        if (auto * nullable = checkAndGetColumn<ColumnNullable>(*elem.column)) {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto & src_data = nullable->getNullMapData();
            auto & res_data = assert_cast<ColumnUInt8 &>(*res_column).getData();

            for (size_t i = 0; i < input_rows_count; ++i)
                res_data[i] = !src_data[i];

            block.getByPosition(result).column = std::move(res_column);
        }
        else {
            /// Since no element is nullable, return a constant one.
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(elem.column->size(), 1u);
        }
        return Status::OK();
    }
};

void registerFunctionIsNotNull(SimpleFunctionFactory & factory) {
    factory.registerFunction<FunctionIsNotNull>();
}

}