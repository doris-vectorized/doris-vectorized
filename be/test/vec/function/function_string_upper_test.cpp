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

TEST(function_string_upper_test, function_string_upper_test) {
    int len = 5;
    std::string str_test[len] = {"", "asd", "hello123", "HELLO,!^%", "MYtestStr"};
    std::string str_expected[len] = {"", "ASD", "HELLO123", "HELLO,!^%", "MYTESTSTR"};

    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < len; ++i) {
        strcol->insert_data(str_test[i].c_str(), str_test[i].size());
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, "k1");
    vectorized::Block block({type_and_name});

    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));

    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1"))};

    doris::vectorized::SimpleFunctionFactory factory;
    doris::vectorized::register_function_string(factory);
    auto str_function =
            factory.get_function("upper", ctn, std::make_shared<vectorized::DataTypeString>());

    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, block.get_by_position(0).type, "upper(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[1];
        ASSERT_STREQ(column->get_data_at(i).data, str_expected[i].c_str());
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
