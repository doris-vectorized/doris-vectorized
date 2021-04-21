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

#include <ostream>

#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class DataTypeString final : public IDataType {
public:
    using FieldType = String;
    static constexpr bool is_parametric = false;

    const char* getFamilyName() const override { return "String"; }

    TypeIndex getTypeId() const override { return TypeIndex::String; }

    //    void serializeBinary(const Field & field, WriteBuffer & ostr) const override;
    //    void deserializeBinary(Field & field, ReadBuffer & istr) const override;
    //    void serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override;
    //    void deserializeBinary(IColumn & column, ReadBuffer & istr) const override;
    //
    //    void serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const override;
    //    void deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const override;
    //
    //    void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //    void deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    //
    //    void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //    void deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    //
    //    void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //    void deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    //
    //    void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //    void deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    //
    //    void serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //
    //    void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings &) const override;
    //    void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings &) const override;
    //
    //    void serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const override;
    //    void deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const override;
    void serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType& rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeComparedWithCollation() const override { return true; }
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool isCategorial() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }
};

} // namespace doris::vectorized
