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

#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include <vec/aggregate_functions/factory_helpers.h>
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include <vec/aggregate_functions/helpers.h>

namespace doris::vectorized {

namespace {

/// min, max
template <template <typename, bool> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
    #define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>, false>(argument_type);
        FOR_NUMERIC_TYPES(DISPATCH)
    #undef DISPATCH

    // if (which.idx == TypeIndex::Date)
    //     return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>, false>(argument_type);
    // if (which.idx == TypeIndex::DateTime)
    //     return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>, false>(argument_type);
    // if (which.idx == TypeIndex::String)
    //     return new AggregateFunctionTemplate<Data<SingleValueDataString>, true>(argument_type);

    // return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>, false>(argument_type);
    return nullptr;
}

AggregateFunctionPtr createAggregateFunctionMax(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMaxData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types, parameters));
}

} // namespace

void registerAggregateFunctionMinMax(AggregateFunctionSimpleFactory& factory) {
    factory.registerFunction("max", createAggregateFunctionMax);
    factory.registerFunction("max", createAggregateFunctionMax, true);
    factory.registerFunction("min", createAggregateFunctionMin);
    factory.registerFunction("min", createAggregateFunctionMin, true);
}

} // namespace doris::vectorized