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

#include <vec/columns/column_nullable.h>

#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

template <typename Transform>
class FunctionDateTimeStringToString : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create() { return std::make_shared<FunctionDateTimeStringToString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override {
        return makeNullable(std::make_shared<DataTypeString>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) override {
        const ColumnPtr source_col = block.getByPosition(arguments[0]).column;

        const auto* nullable_column = checkAndGetColumn<ColumnNullable>(source_col.get());
        const auto* sources = checkAndGetColumn<ColumnVector<typename Transform::FromType>>(
                nullable_column ? nullable_column->getNestedColumnPtr().get() : source_col.get());

        if (sources) {
            auto col_res = ColumnString::create();
            ColumnUInt8::MutablePtr col_null_map_to;
            col_null_map_to = ColumnUInt8::create(sources->size());
            auto& vec_null_map_to = col_null_map_to->getData();

            if (arguments.size() == 2) {
                const IColumn& source_col1 = *block.getByPosition(arguments[1]).column;
                if (const auto* delta_const_column =
                            typeid_cast<const ColumnConst*>(&source_col1)) {
                    TransformerToStringTwoArgument<Transform>::vector_constant(
                            sources->getData(), delta_const_column->getField().get<String>(),
                            col_res->getChars(), col_res->getOffsets(), vec_null_map_to);
                } else {
                    return Status::InternalError(
                            "Illegal column " +
                            block.getByPosition(arguments[1]).column->getName() + " is not const" +
                            name);
                }
            } else {
                TransformerToStringTwoArgument<Transform>::vector_constant(
                        sources->getData(), "yyyy-MM-dd HH:mm:ss", col_res->getChars(),
                        col_res->getOffsets(), vec_null_map_to);
            }

            if (nullable_column) {
                const auto& origin_null_map = nullable_column->getNullMapColumn().getData();
                for (int i = 0; i < origin_null_map.size(); ++i) {
                    vec_null_map_to[i] |= origin_null_map[i];
                }
            }
            block.getByPosition(result).column =
                    ColumnNullable::create(std::move(col_res), std::move(col_null_map_to));
        } else {
            return Status::InternalError("Illegal column " +
                                         block.getByPosition(arguments[0]).column->getName() +
                                         " of first argument of function " + name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
