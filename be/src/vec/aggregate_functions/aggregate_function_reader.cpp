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

#include "vec/aggregate_functions/aggregate_function_reader.h"

namespace doris::vectorized {

void register_aggregate_function_reader(AggregateFunctionSimpleFactory& factory) {
    auto register_function_reader = [&](const std::string& name,
                                        const AggregateFunctionCreator& creator,
                                        bool nullable = false) {
        factory.register_function(name + agg_reader_suffix, creator, nullable);
    };

    register_function_reader("sum", create_aggregate_function_sum_reader);
    register_function_reader("max", create_aggregate_function_max);
    register_function_reader("min", create_aggregate_function_min);
    //TODO register_function_reader("replace", );
    //TODO register_function_reader("replace_if_not_null", );
    register_function_reader("bitmap_union", create_aggregate_function_bitmap_union);
    register_function_reader("hll_union", create_aggregate_function_HLL_union);
}

} // namespace doris::vectorized