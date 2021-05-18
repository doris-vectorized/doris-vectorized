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

#include "vec/data_types/data_type_nullable.h"

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes

DataTypeNullable::DataTypeNullable(const DataTypePtr& nested_data_type_)
        : nested_data_type{nested_data_type_} {
    if (!nested_data_type->canBeInsideNullable())
        throw Exception(
                "Nested type " + nested_data_type->getName() + " cannot be inside Nullable type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

bool DataTypeNullable::onlyNull() const {
    return typeid_cast<const DataTypeNothing*>(nested_data_type.get());
}

std::string DataTypeNullable::to_string(const IColumn& column, size_t row_num) const {
    const ColumnNullable& col =
            assert_cast<const ColumnNullable&>(*column.convertToFullColumnIfConst().get());

    if (col.isNullAt(row_num)) {
        return "\\N";
    } else {
        return nested_data_type->to_string(col.getNestedColumn(), row_num);
    }
}

void DataTypeNullable::serialize(const IColumn& column, PColumn* pcolumn) const {
    const ColumnNullable& col =
            assert_cast<const ColumnNullable&>(*column.convertToFullColumnIfConst().get());
    for (size_t i = 0; i < column.size(); ++i) {
        bool is_null = col.isNullAt(i);
        pcolumn->add_is_null(is_null);
    }
    nested_data_type->serialize(col.getNestedColumn(), pcolumn);
}

void DataTypeNullable::deserialize(const PColumn& pcolumn, IColumn* column) const {
    ColumnNullable* col = assert_cast<ColumnNullable*>(column);
    for (int i = 0; i < pcolumn.is_null_size(); ++i) {
        if (pcolumn.is_null(i)) {
            col->getNullMapData().push_back(1);
        } else {
            col->getNullMapData().push_back(0);
        }
    }
    IColumn& nested = col->getNestedColumn();
    nested_data_type->deserialize(pcolumn, &nested);
}

MutableColumnPtr DataTypeNullable::createColumn() const {
    return ColumnNullable::create(nested_data_type->createColumn(), ColumnUInt8::create());
}

Field DataTypeNullable::getDefault() const {
    return Null();
}

size_t DataTypeNullable::getSizeOfValueInMemory() const {
    throw Exception("Value of type " + getName() + " in memory is not of fixed size.",
                    ErrorCodes::LOGICAL_ERROR);
}

bool DataTypeNullable::equals(const IDataType& rhs) const {
    return rhs.isNullable() &&
           nested_data_type->equals(*static_cast<const DataTypeNullable&>(rhs).nested_data_type);
}

DataTypePtr makeNullable(const DataTypePtr& type) {
    if (type->isNullable()) return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr removeNullable(const DataTypePtr& type) {
    if (type->isNullable()) return static_cast<const DataTypeNullable&>(*type).getNestedType();
    return type;
}

} // namespace doris::vectorized
