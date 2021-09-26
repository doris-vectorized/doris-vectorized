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

#ifndef DORIS_FUNCTION_IFNULL_H
#define DORIS_FUNCTION_IFNULL_H

#include "vec/functions/simple_function_factory.h"
#include "vec/columns/column_nullable.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"
#include "vec/functions/function_string.h"

namespace doris::vectorized {
class FunctionIfNull : public IFunction {
public:
    static constexpr auto name = "ifnull";

    static FunctionPtr create() { return std::make_shared<FunctionIfNull>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }
    //TODO:
    bool use_default_implementation_for_constants() const override { return false; }

    bool use_default_implementation_for_nulls() const override { return false; }

    // ifnull(col_left, col_right) == if(isnotnull(col_left), col_left, col_right)
    Status execute_impl(Block &block, const ColumnNumbers &arguments, size_t result,
                        size_t input_rows_count) override {
        const ColumnWithTypeAndName& col_left = block.get_by_position(arguments[0]);

        if (col_left.column->only_null()) {
            block.get_by_position(result).column = block.get_by_position(arguments[1]).column;
            return Status::OK();
        }
        const ColumnWithTypeAndName new_result_column {
            block.get_by_position(result),
        };
        const ColumnWithTypeAndName new_column {
            col_left,
        };
        /// compute isnotnull(col_left)
        if (auto* nullable = check_and_get_column<ColumnNullable>(*col_left.column)) {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto& src_data = nullable->get_null_map_data();
            auto& res_data = assert_cast<ColumnUInt8&>(*res_column).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                res_data[i] = !src_data[i];
            }

            block.replace_by_position(arguments[0], std::move(res_column));
        } else {
            block.get_by_position(arguments[0]).column = col_left.column;
            return Status::OK();
        }

        const ColumnsWithTypeAndName if_columns
        {
            block.get_by_position(arguments[0]),
            block.get_by_position(arguments[1]),
            new_column,
        };

        Block temporary_block(
                {block.get_by_position(arguments[0]),
                 new_column,
                 block.get_by_position(arguments[1]),
                 new_result_column
                });
        auto func_if = SimpleFunctionFactory::instance().get_function("if", if_columns, new_result_column.type);
        func_if->execute(temporary_block, {0, 1, 2}, 3, input_rows_count);
        /// need to handle nullable type and not nullable type differently,
        /// because `IF` function always return nullable type, but result type is not always
        if (block.get_by_position(result).type->is_nullable()) {
            block.get_by_position(result).column = temporary_block.get_by_position(3).column;
        } else {
            auto cols = check_and_get_column<ColumnNullable>(temporary_block.get_by_position(3).column.get());
            block.replace_by_position(result, std::move(cols->get_nested_column_ptr()));
        }
        return Status::OK();
    }
};
}
#endif //DORIS_FUNCTION_IFNULL_H