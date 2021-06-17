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
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/data_types/data_type_number.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {
    
/// Implements the function ifNull which takes 2 arguments and returns
/// the value of the 1st argument if it is not null. Otherwise it returns
/// the value of the 2nd argument.
class FunctionIfNull : public IFunction {
public:
    static constexpr auto name = "ifnull";

    FunctionIfNull() {}

    static FunctionPtr create() {
        return std::make_shared<FunctionIfNull>();
    }

    std::string getName() const override {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override {
        if (arguments[0]->onlyNull())
            return arguments[1];

        if (!arguments[0]->isNullable())
            return arguments[0];

        return getLeastSupertype({removeNullable(arguments[0]), arguments[1]});
    }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override {
        /// Always null.
        if (block.getByPosition(arguments[0]).type->onlyNull()) {
            block.getByPosition(result).column = block.getByPosition(arguments[1]).column;
            return Status::OK();
        }

        /// Could not contain nulls, so nullIf makes no sense.
        if (!block.getByPosition(arguments[0]).type->isNullable()) {
            block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
            return Status::OK();
        }

        /// ifNull(col1, col2) == if(isNotNull(col1), assumeNotNull(col1), col2)

        Block temp_block = block;

        size_t is_not_null_pos = temp_block.columns();
        temp_block.insert({nullptr, std::make_shared<DataTypeUInt8>(), ""});
        auto is_not_null = SimpleFunctionFactory::instance().get_function("is_not_null_pred", {temp_block.getByPosition(arguments[0])});
        is_not_null->execute(temp_block, {arguments[0]}, is_not_null_pos, input_rows_count);

        size_t assume_not_null_pos = temp_block.columns();
        const ColumnPtr& col = block.getByPosition(arguments[0]).column;
        if (auto * nullable_col = checkAndGetColumn<ColumnNullable>(*col)) {
            temp_block.insert({nullable_col->getNestedColumnPtr(), removeNullable(block.getByPosition(arguments[0]).type), ""});
        } else {
            temp_block.insert({col, removeNullable(block.getByPosition(arguments[0]).type), ""});
        }

        auto func_if = SimpleFunctionFactory::instance().get_function("if", 
            {temp_block.getByPosition(is_not_null_pos), temp_block.getByPosition(assume_not_null_pos), temp_block.getByPosition(arguments[1])});
        
        func_if->execute(temp_block, {is_not_null_pos, assume_not_null_pos, arguments[1]}, result, input_rows_count);

        block.getByPosition(result).column = std::move(temp_block.getByPosition(result).column);

        return Status::OK();
    }
};

void registerFunctionIfNull(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionIfNull>();
}

} // namespace