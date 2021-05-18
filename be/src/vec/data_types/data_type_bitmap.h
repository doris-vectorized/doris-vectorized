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
#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
class DataTypeBitMap : public IDataType {
public:
    DataTypeBitMap() = default;
    ~DataTypeBitMap() = default;

    std::string doGetName() const override { return "BitMap"; }

    TypeIndex getTypeId() const override { return TypeIndex::BitMap; }

    void serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
    MutableColumnPtr createColumn() const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override { return true; }
    bool isComparable() const override { return false; }
    bool isValueRepresentedByNumber() const override { return false; }
    bool isValueRepresentedByInteger() const override { return false; }
    bool isValueRepresentedByUnsignedInteger() const override { return false; }
    // TODO:
    bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const override { return true; }
    bool haveMaximumSizeOfValue() const override { return true; }
    size_t getSizeOfValueInMemory() const override { return sizeof(BitmapValue); }

    bool canBeUsedAsVersion() const override { return false; }

    bool canBeInsideNullable() const override { return true; }

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    bool isCategorial() const override { return isValueRepresentedByInteger(); }

    bool canBeInsideLowCardinality() const override { return false; }

    std::string to_string(const IColumn& column, size_t row_num) const { return "BitMap()"; }
};
} // namespace doris::vectorized
