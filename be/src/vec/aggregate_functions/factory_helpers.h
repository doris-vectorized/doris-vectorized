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

#include <vec/core/field.h>
#include <vec/data_types/data_type.h>
// #include <IO/WriteHelpers.h>

namespace doris::vectorized {

namespace ErrorCodes {
extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

inline void assertNoParameters(const std::string& name, const Array& parameters) {
    if (!parameters.empty())
        throw Exception("Aggregate function " + name + " cannot have parameters",
                        ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
}

inline void assertUnary(const std::string& name, const DataTypes& argument_types) {
    if (argument_types.size() != 1)
        throw Exception("Aggregate function " + name + " require single argument",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

inline void assertBinary(const std::string& name, const DataTypes& argument_types) {
    if (argument_types.size() != 2)
        throw Exception("Aggregate function " + name + " require two arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

template <std::size_t maximal_arity>
inline void assertArityAtMost(const std::string& name, const DataTypes& argument_types) {
    if (argument_types.size() <= maximal_arity) return;

    if constexpr (maximal_arity == 0)
        throw Exception("Aggregate function " + name + " cannot have arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if constexpr (maximal_arity == 1)
        throw Exception("Aggregate function " + name + " requires zero or one argument",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    throw Exception("Aggregate function " + name + " requires at most " +
                            std::to_string(maximal_arity) + " arguments",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}

} // namespace doris::vectorized
