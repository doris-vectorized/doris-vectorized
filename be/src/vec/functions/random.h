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

#ifndef DORIS_RANDOM_H
#define DORIS_RANDOM_H

#include "vec/functions/simple_function_factory.h"
#include "vec/functions/function_helpers.h"
#include "udf/udf.h"
#include <random>

namespace doris::vectorized {
class Random : public IFunction {
public:
    static constexpr auto name = "random";

    static FunctionPtr create() {
        return std::make_shared<Random>();
    }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }
    // TODO: Currently seed is ignored, rand(seed) is the same as rand()
    // Because this function will be called multiple times for one query,
    // rand(seed) will return duplicated results
    Status execute_impl(Block &block, const ColumnNumbers &arguments, size_t result,
                        size_t input_rows_count) override {
        static const double min = 0.0;
        static const double max = 1.0;
        auto res_column = ColumnFloat64::create(input_rows_count);
        auto& res_data = assert_cast<ColumnFloat64&>(*res_column).get_data();

        std::mt19937_64 generator(std::random_device{}());
        std::uniform_real_distribution<double> distribution(min, max);
        for (int i = 0; i < input_rows_count; i++) {
            res_data[i] = distribution(generator);
        }
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};
}

#endif //DORIS_RANDOM_H
