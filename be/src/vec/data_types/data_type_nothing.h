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

#include <vec/common/exception.h>

#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

/** Data type that cannot have any values.
  * Used to represent NULL of unknown type as Nullable(Nothing),
  * and possibly for empty array of unknown type as Array(Nothing).
  */
class DataTypeNothing final : public IDataType {
public:
    static constexpr bool is_parametric = false;

    const char* getFamilyName() const override { return "Nothing"; }
    TypeIndex getTypeId() const override { return TypeIndex::Nothing; }

    MutableColumnPtr createColumn() const override;

    /// These methods read and write zero bytes just to allow to figure out size of column.
    // void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    // void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;

    bool equals(const IDataType& rhs) const override;

    bool isParametric() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return 0; }
    bool canBeInsideNullable() const override { return true; }

    void serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
    Field getDefault() const override {
        throw Exception("Method getDefault() is not implemented for data type " + getName(),
                        ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefaultInto(IColumn&) const override {
        throw Exception("Method insertDefaultInto() is not implemented for data type " + getName(),
                        ErrorCodes::NOT_IMPLEMENTED);
    }

    bool haveSubtypes() const override { return false; }
    bool cannotBeStoredInTables() const override { return true; }
};

} // namespace doris::vectorized
