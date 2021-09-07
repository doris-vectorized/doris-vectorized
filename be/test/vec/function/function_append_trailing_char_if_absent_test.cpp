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

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

TEST(function_append_trailing_char_if_absent_test, function_append_trailing_char_if_absent_test) {
    std::vector<std::string> str;
    std::vector<std::string> trailing_char;
    std::vector<std::string> str_expected;
    std::vector<bool> null_expected;

    str.push_back("ASD");
    trailing_char.push_back("D");
    str_expected.push_back("ASD");
    null_expected.push_back(0);

    str.push_back("AS");
    trailing_char.push_back("D");
    str_expected.push_back("ASD");
    null_expected.push_back(0);

    str.push_back("ASD");
    trailing_char.push_back("");
    str_expected.push_back("");
    null_expected.push_back(1);

    str.push_back("ASD");
    trailing_char.push_back("AB");
    str_expected.push_back("");
    null_expected.push_back(1);

    str.push_back("");
    trailing_char.push_back("");
    str_expected.push_back("");
    null_expected.push_back(1);

    str.push_back("");
    trailing_char.push_back("AB");
    str_expected.push_back("");
    null_expected.push_back(1);

    str.push_back("");
    trailing_char.push_back("A");
    str_expected.push_back("A");
    null_expected.push_back(0);

    str.push_back("我");
    trailing_char.push_back("A");
    str_expected.push_back("我A");
    null_expected.push_back(0);

    int len = str.size();

    auto str_col = vectorized::ColumnString::create();
    auto trailing_char_col = vectorized::ColumnString::create();
    for (int i = 0; i < len; ++i) {
        str_col->insert_data(str[i].c_str(), str[i].size());
        trailing_char_col->insert_data(trailing_char[i].c_str(), trailing_char[i].size());
    }

    vectorized::ColumnWithTypeAndName str_type_and_name(
            str_col->get_ptr(), std::make_shared<vectorized::DataTypeString>(), "k1");

    vectorized::ColumnWithTypeAndName trailing_char_data_type_and_name(
            trailing_char_col->get_ptr(), std::make_shared<vectorized::DataTypeString>(), "k2");

    vectorized::Block block({str_type_and_name, trailing_char_data_type_and_name});

    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));
    arguments.emplace_back(block.get_position_by_name("k2"));

    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1")),
            block.get_by_position(block.get_position_by_name("k2"))};

    auto str_function = doris::vectorized::SimpleFunctionFactory::instance().get_function(
            "append_trailing_char_if_absent", ctn,
            make_nullable(std::make_shared<vectorized::DataTypeString>()));

    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, block.get_by_position(0).type, "append_trailing_char_if_absent(k1,k2)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[num_columns_without_result];
        ASSERT_EQ(column->is_null_at(i), null_expected[i]);
        if (!null_expected[i]) {
            vectorized::Field field;
            column->get(i, field);
            const std::string& str = field.get<std::string>();
            ASSERT_STREQ(str_expected[i].c_str(),str.c_str());
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}