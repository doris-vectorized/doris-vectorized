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
void registerFunctionHLLCardinality(SimpleFunctionFactory& factory);
void registerFunctionHLLEmpty(SimpleFunctionFactory& factory);
void registerFunctionHLLHash(SimpleFunctionFactory& factory);
void registerFunctionLogical(SimpleFunctionFactory& factory);
void registerFunctionCast(SimpleFunctionFactory& factory);
void registerFunctionPlus(SimpleFunctionFactory& factory);
void registerFunctionMinus(SimpleFunctionFactory& factory);
void registerFunctionMultiply(SimpleFunctionFactory& factory);
void registerFunctionDivide(SimpleFunctionFactory& factory);
void registerFunctionIntDiv(SimpleFunctionFactory& factory);
void registerFunctionMath(SimpleFunctionFactory& factory);
void registerFunctionModulo(SimpleFunctionFactory& factory);
void registerFunctionBitmap(SimpleFunctionFactory& factory);
void registerFunctionIsNull(SimpleFunctionFactory& factory);
void registerFunctionIsNotNull(SimpleFunctionFactory& factory);
void registerFunctionToTimeFuction(SimpleFunctionFactory& factory);
void registerFunctionTimeOfFuction(SimpleFunctionFactory& factory);
void registerFunctionString(SimpleFunctionFactory& factory);
void registerFunctionDateTimeToString(SimpleFunctionFactory& factory);
void registerFunctionDateTimeStringToString(SimpleFunctionFactory& factory);
void registerFunctionIn(SimpleFunctionFactory& factory);
void registerFunctionIf(SimpleFunctionFactory& factory);
void registerFunctionDateTimeComputation(SimpleFunctionFactory& factory);
void registerFunctionStrToDate(SimpleFunctionFactory& factory);
void registerFunctionJson(SimpleFunctionFactory& factory);

class SimpleFunctionFactory {
    using Creator = std::function<FunctionBuilderPtr()>;
    using FunctionCreators = std::unordered_map<std::string, Creator>;

public:
    void registerFunction(const std::string& name, Creator ptr) { function_creators[name] = ptr; }

    template <class Function>
    void registerFunction() {
        if constexpr (std::is_base_of<IFunction, Function>::value)
            registerFunction(Function::name, &createDefaultFunction<Function>);
        else
            registerFunction(Function::name, &Function::create);
    }

    void registerAlias(const std::string& name, const std::string& alias) {
        function_creators[alias] = function_creators[name];
    }

    FunctionBasePtr get_function(const std::string& name, const ColumnsWithTypeAndName& arguments) {
        auto iter = function_creators.find(name);
        if (iter != function_creators.end()) {
            return iter->second()->build(arguments);
        }
        return nullptr;
    }

private:
    FunctionCreators function_creators;

    template <typename Function>
    static FunctionBuilderPtr createDefaultFunction() {
        return std::make_shared<DefaultFunctionBuilder>(Function::create());
    }

public:
    static SimpleFunctionFactory& instance() {
        static std::once_flag oc;
        static SimpleFunctionFactory instance;
        std::call_once(oc, [&]() {
            registerFunctionBitmap(instance);
            registerFunctionHLLCardinality(instance);
            registerFunctionHLLEmpty(instance);
            registerFunctionHLLHash(instance);
            registerFunctionComparison(instance);
            registerFunctionLogical(instance);
            registerFunctionCast(instance);
            registerFunctionPlus(instance);
            registerFunctionMinus(instance);
            registerFunctionMath(instance);
            registerFunctionMultiply(instance);
            registerFunctionDivide(instance);
            registerFunctionIntDiv(instance);
            registerFunctionModulo(instance);
            registerFunctionIsNull(instance);
            registerFunctionIsNotNull(instance);
            registerFunctionToTimeFuction(instance);
            registerFunctionTimeOfFuction(instance);
            registerFunctionString(instance);
            registerFunctionIn(instance);
            registerFunctionIf(instance);
            registerFunctionDateTimeComputation(instance);
            registerFunctionStrToDate(instance);
            registerFunctionDateTimeToString(instance);
            registerFunctionDateTimeStringToString(instance);
            registerFunctionJson(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
