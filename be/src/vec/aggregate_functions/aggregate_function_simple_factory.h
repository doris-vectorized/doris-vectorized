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

#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
using AggregateFunctionCreator =
        std::function<AggregateFunctionPtr(const std::string&, const DataTypes&, const Array&)>;

class AggregateFunctionSimpleFactory {
public:
    using Creator = AggregateFunctionCreator;

private:
    using AggregateFunctions = std::unordered_map<std::string, Creator>;

    AggregateFunctions aggregate_functions;
    AggregateFunctions nullable_aggregate_functions;

public:
    void registerNullableFunctionCombinator(Creator creator) {
        for (const auto& entity : aggregate_functions) {
            if (nullable_aggregate_functions[entity.first] == nullptr) {
                nullable_aggregate_functions[entity.first] = creator;
            }
        }
    }

    void register_distinct_function_combinator(Creator creator, const std::string& prefix) {
        std::vector<std::string> need_insert;
        for (auto entity : aggregate_functions) {
            std::string target_value = prefix + entity.first;
            if (aggregate_functions[target_value] == nullptr) {
                need_insert.emplace_back(std::move(target_value));
            }
        }
        for (const auto& function_name : need_insert) {
            aggregate_functions[function_name] = creator;
        }
    }

    void register_function(const std::string& name, Creator creator, bool nullable = false) {
        if (nullable) {
            nullable_aggregate_functions[name] = creator;
        } else {
            aggregate_functions[name] = creator;
        }
    }

    AggregateFunctionPtr get(const std::string& name, const DataTypes& argument_types,
                             const Array& parameters) {
        bool nullable = false;
        for (const auto& type : argument_types) {
            if (type->is_nullable()) {
                nullable = true;
            }
        }
        if (nullable) {
            return nullable_aggregate_functions[name] == nullptr
                           ? nullptr
                           : nullable_aggregate_functions[name](name, argument_types, parameters);
        } else {
            return aggregate_functions[name] == nullptr
                           ? nullptr
                           : aggregate_functions[name](name, argument_types, parameters);
        }
    }

public:
    static AggregateFunctionSimpleFactory& instance();
};
}; // namespace doris::vectorized
