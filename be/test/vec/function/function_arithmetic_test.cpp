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

#include <string>

#include "function_test_util.h"
#include "runtime/tuple_row.h"
#include "util/aes_util.h"
#include "util/url_coding.h"
#include "vec/core/field.h"

namespace doris {

using vectorized::Null;
using vectorized::DataSet;
using vectorized::TypeIndex;


TEST(function_arithmetic_test, function_arithmetic_mod_test) {
    std::string func_name = "mod";

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32,
                                             vectorized::TypeIndex::Int32};

        DataSet data_set = {{{10,   1},  0},
                            {{10,   -2}, 0},
                            {{1234, 33}, 13},
                            {{1234, 0},  Null()}};

        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
    }
//
//    {
//        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
//                                             vectorized::TypeIndex::Float64};
//
//        DataSet data_set = {{{10.0,      1.0},  0},
//                            {{10.3,      -2.0}, 0.2},
//                            {{1234.0,    33.0}, 13},
//                            {{1234.0,    33.1}, 9.3},
//                            {{1234.1223, 0.0},  Null()}};
//
//        vectorized::check_function<vectorized::DataTypeFloat64 , true>(func_name, input_types, data_set);
//    }
}

TEST(function_arithmetic_test, function_arithmetic_divide_test) {
    std::string func_name = "divide";

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32,
                                             vectorized::TypeIndex::Int32};
        DataSet data_set = {{{1234, 34}, 36.294117647058826},
                            {{1234, 0}, Null()}};
        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
    }

//    {
//        std::vector<std::any> input_types = {vectorized::TypeIndex::Int32,
//                                             vectorized::TypeIndex::Int32};
//        DataSet data_set = {{{10, 1}, 10},
//                            {{10, -2}, -5},
//                            {{1234, 0}, Null()}};
//        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
//    }

//    {
//        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
//                                             vectorized::TypeIndex::Float64};
//        DataSet data_set = {{{10.0, 1.0}, 10},
//                            {{10.0, -2.0}, -5},
//                            {{1234.34, 0.0}, Null()}};
//        vectorized::check_function<vectorized::DataTypeInt32, true>(func_name, input_types, data_set);
//    }

    {
        std::vector<std::any> input_types = {vectorized::TypeIndex::Float64,
                                             vectorized::TypeIndex::Float64};
        DataSet data_set = {{{1234.1, 34.6}, 35.667630057803464},
                            {{1234.34, 0.0}, Null()}};
        vectorized::check_function<vectorized::DataTypeFloat64, true>(func_name, input_types, data_set);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
