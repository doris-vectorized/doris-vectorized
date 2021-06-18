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

#include "common/logging.h"
#include <vec/aggregate_functions/aggregate_function.h>
#include <vec/aggregate_functions/aggregate_function_combinator.h>
#include <vec/aggregate_functions/aggregate_function_count.h>
#include <vec/aggregate_functions/aggregate_function_nothing.h>
#include <vec/aggregate_functions/aggregate_function_null.h>
#include <vec/data_types/data_type_nullable.h>
#include <vec/aggregate_functions/aggregate_function_simple_factory.h>

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class AggregateFunctionCombinatorNull final : public IAggregateFunctionCombinator {
public:
    String get_name() const override { return "Null"; }

    bool isForInternalUsageOnly() const override { return true; }

    DataTypes transformArguments(const DataTypes& arguments) const override {
        size_t size = arguments.size();
        DataTypes res(size);
        for (size_t i = 0; i < size; ++i) res[i] = removeNullable(arguments[i]);
        return res;
    }

    AggregateFunctionPtr transformAggregateFunction(const AggregateFunctionPtr& nested_function,
                                                    const DataTypes& arguments,
                                                    const Array& params) const override {
        bool has_nullable_types = false;
        bool has_null_types = false;
        for (const auto& arg_type : arguments) {
            if (arg_type->is_nullable()) {
                has_nullable_types = true;
                if (arg_type->only_null()) {
                    has_null_types = true;
                    break;
                }
            }
        }

        if (!has_nullable_types) {
            LOG(WARNING) << fmt::format("Aggregate function combinator 'Null' requires at least one argument to be Nullable");
            return nullptr;
        }

        /// Special case for 'count' function. It could be called with Nullable arguments
        /// - that means - count number of calls, when all arguments are not NULL.
        if (nested_function && nested_function->get_name() == "count")
            return std::make_shared<AggregateFunctionCountNotNullUnary>(arguments[0], params);

        if (has_null_types) return std::make_shared<AggregateFunctionNothing>(arguments, params);

        bool return_type_is_nullable = nested_function->getReturnType()->can_be_inside_nullable();

        if (arguments.size() == 1) {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullUnary<true>>(nested_function,
                                                                          arguments, params);
            else
                return std::make_shared<AggregateFunctionNullUnary<false>>(nested_function,
                                                                           arguments, params);
        } else {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullVariadic<true>>(nested_function,
                                                                             arguments, params);
            else
                return std::make_shared<AggregateFunctionNullVariadic<false>>(nested_function,
                                                                              arguments, params);
        }
    }
};

void registerAggregateFunctionCombinatorNull(AggregateFunctionSimpleFactory& factory) {
    // factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const Array& params) {
        auto function_combinator = std::make_shared<AggregateFunctionCombinatorNull>();
        auto transformArguments = function_combinator->transformArguments(types);
        auto nested_function = factory.get(name, transformArguments, params);
        return function_combinator->transformAggregateFunction(nested_function, types, params);
    };
    factory.registerNullableFunctionCombinator(creator);
}

} // namespace doris::vectorized
