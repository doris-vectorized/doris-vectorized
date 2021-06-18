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

#include "vec/data_types/data_type.h"

namespace doris::vectorized {

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType {
public:
    static constexpr bool is_parametric = true;

    explicit DataTypeNullable(const DataTypePtr& nested_data_type_);
    std::string doGetName() const override {
        return "Nullable(" + nested_data_type->get_name() + ")";
    }
    const char* get_family_name() const override { return "Nullable"; }
    TypeIndex getTypeId() const override { return TypeIndex::Nullable; }

    void serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool cannotBeStoredInTables() const override {
        return nested_data_type->cannotBeStoredInTables();
    }
    bool shouldAlignRightInPrettyFormats() const override {
        return nested_data_type->shouldAlignRightInPrettyFormats();
    }
    bool textCanContainOnlyValidUTF8() const override {
        return nested_data_type->textCanContainOnlyValidUTF8();
    }
    bool isComparable() const override { return nested_data_type->isComparable(); }
    bool canBeComparedWithCollation() const override {
        return nested_data_type->canBeComparedWithCollation();
    }
    bool canBeUsedAsVersion() const override { return false; }
    bool isSummable() const override { return nested_data_type->isSummable(); }
    bool canBeUsedInBooleanContext() const override {
        return nested_data_type->canBeUsedInBooleanContext();
    }
    bool haveMaximumSizeOfValue() const override {
        return nested_data_type->haveMaximumSizeOfValue();
    }
    size_t getMaximumSizeOfValueInMemory() const override {
        return 1 + nested_data_type->getMaximumSizeOfValueInMemory();
    }
    bool is_nullable() const override { return true; }
    size_t getSizeOfValueInMemory() const override;
    bool only_null() const override;
    bool canBeInsideLowCardinality() const override {
        return nested_data_type->canBeInsideLowCardinality();
    }
    std::string to_string(const IColumn& column, size_t row_num) const;

    const DataTypePtr& getNestedType() const { return nested_data_type; }

private:
    DataTypePtr nested_data_type;
};

DataTypePtr make_nullable(const DataTypePtr& type);
DataTypePtr removeNullable(const DataTypePtr& type);

} // namespace doris::vectorized
