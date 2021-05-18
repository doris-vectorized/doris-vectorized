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

#include <vec/aggregate_functions/aggregate_function_hll_union_agg.h>
#include <vec/aggregate_functions/factory_helpers.h>

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {

AggregateFunctionPtr createAggregateFunctionHLLUnionAgg(const std::string& name,
                                                  const DataTypes& argument_types,
                                                  const Array& parameters) {
    assertNoParameters(name, parameters);
    assertArityAtMost<1>(name, argument_types);

    return std::make_shared<AggregateFunctionHLLUnionAgg>(argument_types);
}

AggregateFunctionPtr createAggregateFunctionHLLUnion(const std::string& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    assertNoParameters(name, parameters);
    assertArityAtMost<1>(name, argument_types);

    return std::make_shared<AggregateFunctionHLLUnion>(argument_types);
}

void registerAggregateFunctionHLLUnionAgg(AggregateFunctionSimpleFactory& factory) {
    factory.registerFunction("hll_union_agg", createAggregateFunctionHLLUnionAgg);
    factory.registerFunction("hll_union", createAggregateFunctionHLLUnion);
    factory.registerFunction("hll_raw_agg", createAggregateFunctionHLLUnion);
}

} // namespace doris::vectorized
