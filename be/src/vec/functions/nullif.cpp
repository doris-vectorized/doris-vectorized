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
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {
/// Implements the function nullIf which takes 2 arguments and returns
/// NULL if both arguments have the same value. Otherwise it returns the
/// value of the first argument.
class FunctionNullIf : public IFunction {

public:
    static constexpr auto name = "nullif";

    static FunctionPtr create() {
        return std::make_shared<FunctionNullIf>();
    }

    FunctionNullIf() {}

    std::string getName() const override {
        return name;
    }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override {
        return makeNullable(arguments[0]);
    }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override {
        /// nullIf(col1, col2) == if(col1 = col2, NULL, col1)

        Block temp_block = block;

        auto equals_func = SimpleFunctionFactory::instance().get_function("eq", 
            {temp_block.getByPosition(arguments[0]), temp_block.getByPosition(arguments[1])});

        size_t equals_res_pos = temp_block.columns();
        temp_block.insert({nullptr, equals_func->getReturnType(), "equals res"});

        equals_func->execute(temp_block, {arguments[0], arguments[1]}, equals_res_pos, input_rows_count);

        /// Argument corresponding to the NULL value.
        size_t null_pos = temp_block.columns();

        /// Append a NULL column.
        ColumnWithTypeAndName null_elem;
        null_elem.type = block.getByPosition(result).type;
        null_elem.column = null_elem.type->createColumnConstWithDefaultValue(input_rows_count);
        null_elem.name = "NULL";

        temp_block.insert(null_elem);

        auto func_if = SimpleFunctionFactory::instance().get_function("if", 
            {temp_block.getByPosition(equals_res_pos), temp_block.getByPosition(null_pos), temp_block.getByPosition(arguments[0])});
        func_if->execute(temp_block, {equals_res_pos, null_pos, arguments[0]}, result, input_rows_count);

        block.getByPosition(result).column = makeNullable(std::move(temp_block.getByPosition(result).column));

        return Status::OK();
    }
};

void registerFunctionNullIf(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionNullIf>();
}

} // namespace