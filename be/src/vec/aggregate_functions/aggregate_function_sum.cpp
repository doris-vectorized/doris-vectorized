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

// #include <vec/AggregateFunctions/AggregateFunctionFactory.h>
#include "vec/aggregate_functions/aggregate_function_sum.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
// #include <vec/AggregateFunctions/FactoryHelpers.h>

namespace doris::vectorized {

namespace {

template <typename T>
struct SumSimple {
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    // using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;
    using ResultType = NearestFieldType<T>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
struct SumSameType {
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
struct SumKahan {
    using ResultType = Float64;
    using AggregateDataType = AggregateFunctionSumKahanData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;
};

template <typename T>
using AggregateFunctionSumSimple = typename SumSimple<T>::Function;
template <typename T>
using AggregateFunctionSumWithOverflow = typename SumSameType<T>::Function;
template <typename T>
using AggregateFunctionSumKahan =
        std::conditional_t<IsDecimalNumber<T>, typename SumSimple<T>::Function,
                           typename SumKahan<T>::Function>;

template <template <typename> class Function>
AggregateFunctionPtr createAggregateFunctionSum(const std::string& name,
                                                const DataTypes& argument_types,
                                                const Array& parameters) {
    // assertNoParameters(name, parameters);
    // assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<Function>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<Function>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() +
                                " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

} // namespace

// void registerAggregateFunctionSum(AggregateFunctionFactory & factory)
// {
//     factory.registerFunction("sum", createAggregateFunctionSum<AggregateFunctionSumSimple>, AggregateFunctionFactory::CaseInsensitive);
//     factory.registerFunction("sumWithOverflow", createAggregateFunctionSum<AggregateFunctionSumWithOverflow>);
//     factory.registerFunction("sumKahan", createAggregateFunctionSum<AggregateFunctionSumKahan>);
// }

void registerAggregateFunctionSum(AggregateFunctionSimpleFactory& factory) {
    factory.registerFunction("sum", createAggregateFunctionSum<AggregateFunctionSumSimple>);
}

} // namespace doris::vectorized
