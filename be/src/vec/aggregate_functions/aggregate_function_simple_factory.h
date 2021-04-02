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

#include <mutex>
#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class AggregateFunctionSimpleFactory;
void registerAggregateFunctionSum(AggregateFunctionSimpleFactory& factory);

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

public:
    void registerFunction(const std::string& name, Creator creator) {
        aggregate_functions[name] = creator;
    }

    AggregateFunctionPtr get(const std::string& name, const DataTypes& argument_types,
                             const Array& parameters) {
        return aggregate_functions[name](name, argument_types, parameters);
    }
public:
    static AggregateFunctionSimpleFactory& instance() {
        static std::once_flag oc;
        static AggregateFunctionSimpleFactory instance;
        std::call_once(oc, [&]() { 
            registerAggregateFunctionSum(instance);
        });
        return instance;
    }
};
}; // namespace doris::vectorized
