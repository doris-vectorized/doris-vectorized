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

#include "util/string_parser.hpp"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
struct NameToBitmap {
    static constexpr auto name = "to_bitmap";
};

struct ToBitmapImpl {
    using ReturnType = DataTypeBitMap;
    static constexpr auto TYPE_INDEX = TypeIndex::String;
    using Type = String;
    using ReturnColumnType = ColumnBitmap;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         std::vector<BitmapValue>& res) {
        auto size = offsets.size();
        res.reserve(size);
        for (int i = 0; i < size; ++i) {
            const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i]]);
            int str_size = offsets[i] - offsets[i - 1];
            StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
            uint64_t int_value = StringParser::string_to_unsigned_int<uint64_t>(raw_str, str_size,
                                                                                &parse_result);

            if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
                return Status::RuntimeError(
                        fmt::format("The input: {:.{}} is not valid, to_bitmap only support bigint "
                                    "value from 0 to 18446744073709551615 currently",
                                    raw_str, str_size));
            }
            res.emplace_back();
            res.back().add(int_value);
        }
        return Status::OK();
    }
};
// using FunctionToBitmap = FunctionBitmap<ToBitmapImpl, NameToBitmap>;

using FunctionToBitmap = FunctionUnaryToType<ToBitmapImpl, NameToBitmap>;

void registerFunctionBitmap(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionToBitmap>();
}

} // namespace doris::vectorized
