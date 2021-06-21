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

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
//#include<vec/columns/column_fixed_string.h>
//#include<vec/columns/column_array.h>

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

template <typename Impl, typename Name, bool is_injective = false>
class FunctionStringToString : public IFunction {
public:
    static constexpr auto name = Name::name;
    //    static FunctionPtr create(const Context &)
    static FunctionPtr create() { return std::make_shared<FunctionStringToString>(); }

    String get_name() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isInjective(const Block&) override { return is_injective; }

    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        if (!isStringOrFixedString(arguments[0])) {
            LOG(FATAL) << fmt::format("Illegal type {} of argument of function {}",
                                      arguments[0]->get_name(), get_name());
        }

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t /*input_rows_count*/) override {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if (const ColumnString* col = check_and_get_column<ColumnString>(column.get())) {
            auto col_res = ColumnString::create();
            Impl::vector(col->get_chars(), col->get_offsets(), col_res->get_chars(),
                         col_res->get_offsets());
            block.get_by_position(result).column = std::move(col_res);
        }
        //        else if (const ColumnFixedString * col_fixed = check_and_get_column<ColumnFixedString>(column.get()))
        //        {
        //            auto col_res = ColumnFixedString::create(col_fixed->getN());
        //            Impl::vector_fixed(col_fixed->get_chars(), col_fixed->getN(), col_res->get_chars());
        //            block.get_by_position(result).column = std::move(col_res);
        //        }
        else {
            return Status::RuntimeError(fmt::format(
                    "Illegal column {} of argument of function {}",
                    block.get_by_position(arguments[0]).column->get_name(), get_name()));
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized