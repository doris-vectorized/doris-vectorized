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
void check_function(std::string func_name, std::vector<TypeIndex> input_types,
                    std::vector<std::pair<std::vector<std::any>, std::vector<std::any>>> data_set,
                    TypeIndex return_type, bool is_nullable) {
    size_t row_size = data_set.size();
    size_t column_size = input_types.size();

    std::list<ColumnPtr> columns;
    Block block;
    ColumnNumbers arguments;
    ColumnsWithTypeAndName ctn;

    for (int i = 0; i < column_size; i++) {
        TypeIndex tp = input_types[i];
        std::string col_name = "k" + std::to_string(i);

        if (tp == TypeIndex::String) {
            auto col = ColumnString::create();

            for (int j = 0; j < row_size; j++) {
                auto str = std::any_cast<std::string>(data_set[j].first[i]);
                col->insert_data(str.c_str(), str.size());
            }

            columns.emplace_back(std::move(col));

            ColumnWithTypeAndName type_and_name(columns.back()->get_ptr(),
                                                std::make_shared<vectorized::DataTypeString>(),
                                                col_name);
            block.insert(i, type_and_name);
            ctn.emplace_back(type_and_name);

        } else if (tp == TypeIndex::Int32) {
            auto col = ColumnInt32::create();

            for (int j = 0; j < row_size; j++) {
                auto value = std::any_cast<int>(data_set[j].first[i]);
                col->insert_value(value);
            }

            columns.emplace_back(std::move(col));

            ColumnWithTypeAndName type_and_name(columns.back()->get_ptr(),
                                                std::make_shared<vectorized::DataTypeInt32>(),
                                                col_name);
            block.insert(i, type_and_name);
            ctn.emplace_back(type_and_name);

        } else {
            assert(false);
        }
        arguments.push_back(i);
    }

    if (return_type == vectorized::TypeIndex::String) {
        if (is_nullable) {
            auto func = SimpleFunctionFactory::instance().get_function(
                    func_name, ctn, make_nullable(std::make_shared<vectorized::DataTypeString>()));

            block.insert({nullptr, block.get_by_position(0).type, "result"});

            func->execute(block, arguments, block.columns() - 1, row_size);

            for (int i = 0; i < row_size; ++i) {
                ColumnPtr column = block.get_columns()[block.columns() - 1];
                bool is_null = std::any_cast<bool>(data_set[i].second[1]);

                ASSERT_EQ(column->is_null_at(i), is_null);

                if (!is_null) {
                    vectorized::Field field;
                    column->get(i, field);

                    const std::string& str = field.get<std::string>();
                    std::string expect_str = std::any_cast<std::string>(data_set[i].second[0]);

                    ASSERT_STREQ(expect_str.c_str(), str.c_str());
                }
            }
        } else {
            auto func = SimpleFunctionFactory::instance().get_function(
                    func_name, ctn, std::make_shared<vectorized::DataTypeString>());

            block.insert({nullptr, block.get_by_position(0).type, "result"});

            func->execute(block, arguments, block.columns() - 1, row_size);

            for (int i = 0; i < row_size; ++i) {
                ColumnPtr column = block.get_columns()[block.columns() - 1];

                const std::string& str = column->get_data_at(i).data;
                std::string expect_str = std::any_cast<std::string>(data_set[i].second[0]);

                ASSERT_STREQ(expect_str.c_str(), str.c_str());
            }
        }
    } else {
        assert(0);
    }
}
} // namespace vectorized
} // namespace doris