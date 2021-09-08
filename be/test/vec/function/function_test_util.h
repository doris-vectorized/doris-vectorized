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

#include <gtest/gtest.h>
#include <time.h>

#include <any>
#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
namespace vectorized {

template <typename ReturnType, bool nullable = false>
void check_function(const std::string& func_name, const std::vector<TypeIndex>& input_types,
                    const std::vector<std::pair<std::vector<std::any>, std::any>>& data_set) {
    size_t row_size = data_set.size();
    size_t column_size = input_types.size();

    std::list<ColumnPtr> columns;
    Block block;
    ColumnNumbers arguments;
    ColumnsWithTypeAndName ctn;

    // 1. build block and column type and names
    for (int i = 0; i < column_size; i++) {
        TypeIndex tp = input_types[i];
        std::string col_name = "k" + std::to_string(i);

        if (tp == TypeIndex::String) {
            auto col = ColumnString::create();
            auto null_map = ColumnUInt8::create(row_size, false);
            auto& null_map_data = null_map->get_data();
            //null_map_data.resize_fill(row_size, false);
            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert("");
                    continue;
                }
                auto str = std::any_cast<std::string>(data_set[j].first[i]);
                col->insert_data(str.c_str(), str.size());
            }

            columns.emplace_back(ColumnNullable::create(std::move(col), std::move(null_map)));

            ColumnWithTypeAndName type_and_name(
                    columns.back()->get_ptr(),
                    make_nullable(std::make_shared<vectorized::DataTypeString>()), col_name);
            block.insert(i, type_and_name);
            ctn.emplace_back(type_and_name);

        } else if (tp == TypeIndex::Int32) {
            auto col = ColumnInt32::create();
            auto null_map = ColumnUInt8::create(row_size, false);
            auto& null_map_data = null_map->get_data();

            for (int j = 0; j < row_size; j++) {
                if (data_set[j].first[i].type() == typeid(Null)) {
                    null_map_data[j] = true;
                    col->insert_value(0);
                    continue;
                }
                auto value = std::any_cast<int>(data_set[j].first[i]);
                col->insert_value(value);
            }

            columns.emplace_back(ColumnNullable::create(std::move(col), std::move(null_map)));

            ColumnWithTypeAndName type_and_name(
                    columns.back()->get_ptr(),
                    make_nullable(std::make_shared<vectorized::DataTypeInt32>()), col_name);
            block.insert(i, type_and_name);
            ctn.emplace_back(type_and_name);
        } else {
            assert(false);
        }
        arguments.push_back(i);
    }

    // 2. execute function
    auto return_type = nullable ? make_nullable(std::make_shared<ReturnType>())
                                : std::make_shared<ReturnType>();
    auto func = SimpleFunctionFactory::instance().get_function(func_name, ctn, return_type);
    block.insert({nullptr, return_type, "result"});
    func->execute(block, arguments, block.columns() - 1, row_size);

    // 3. check the result of function
    for (int i = 0; i < row_size; ++i) {
        ColumnPtr column = block.get_columns()[block.columns() - 1];
        auto check_column_data = [&]() {
            vectorized::Field field;
            column->get(i, field);

            const auto& column_data = field.get<typename ReturnType::FieldType>();
            const auto& expect_data =
                    std::any_cast<typename ReturnType::FieldType>(data_set[i].second);

            ASSERT_EQ(column_data, expect_data);
        };

        if constexpr (nullable) {
            bool is_null = data_set[i].second.type() == typeid(Null);
            ASSERT_EQ(column->is_null_at(i), is_null);
            if (!is_null) check_column_data();
        } else {
            check_column_data();
        }
    }
}
} // namespace vectorized
} // namespace doris