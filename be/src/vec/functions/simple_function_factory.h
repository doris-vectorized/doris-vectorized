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
#include <string>

#include "vec/functions/function.h"

namespace doris::vectorized {

class SimpleFunctionFactory;

void registerFunctionComparison(SimpleFunctionFactory& factory);
void registerFunctionAbs(SimpleFunctionFactory& factory);
void registerFunctionLogical(SimpleFunctionFactory& factory);

class SimpleFunctionFactory {
    using Functions = std::unordered_map<std::string, FunctionPtr>;

public:
    void registerFunction(const std::string& name, FunctionPtr ptr) { functions[name] = ptr; }
    template <class T>
    void registerFunction() {
        registerFunction(T::name, T::create());
    }

    FunctionPtr get(const std::string& name) { return functions[name]; }

private:
    Functions functions;

public:
    static SimpleFunctionFactory& instance() {
        static std::once_flag oc;
        static SimpleFunctionFactory instance;
        std::call_once(oc, [&]() {
            registerFunctionAbs(instance);
            registerFunctionComparison(instance);
            registerFunctionLogical(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
