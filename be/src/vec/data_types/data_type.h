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

#include <boost/noncopyable.hpp>
#include <memory>

#include "vec/common/cow.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"

namespace doris {
class PBlock;
class PColumn;
namespace vectorized {

// class ReadBuffer;
// class WriteBuffer;

class IDataType;
struct FormatSettings;

class IColumn;
using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;

class Field;

using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;

class ProtobufReader;
class ProtobufWriter;

/** Properties of data type.
  * Contains methods for serialization/deserialization.
  * Implementations of this interface represent a data type (example: UInt8)
  *  or parametric family of data types (example: Array(...)).
  *
  * DataType is totally immutable object. You can always share them.
  */
class IDataType : private boost::noncopyable {
public:
    IDataType();
    virtual ~IDataType();

    /// Name of data type (examples: UInt64, Array(String)).
    String getName() const;

    /// Name of data type family (example: FixedString, Array).
    virtual const char* getFamilyName() const = 0;

    /// Data type id. It's used for runtime type checks.
    virtual TypeIndex getTypeId() const = 0;

    virtual void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const;
    virtual std::string to_string(const IColumn& column, size_t row_num) const;

protected:
    virtual String doGetName() const;

public:
    /** Create empty column for corresponding type.
      */
    virtual MutableColumnPtr createColumn() const = 0;

    /** Create ColumnConst for corresponding type, with specified size and value.
      */
    ColumnPtr createColumnConst(size_t size, const Field& field) const;
    ColumnPtr createColumnConstWithDefaultValue(size_t size) const;

    /** Get default value of data type.
      * It is the "default" default, regardless the fact that a table could contain different user-specified default.
      */
    virtual Field getDefault() const = 0;

    /** The data type can be promoted in order to try to avoid overflows.
      * Data types which can be promoted are typically Number or Decimal data types.
      */
    virtual bool canBePromoted() const { return false; }

    /** Return the promoted numeric data type of the current data type. Throw an exception if `canBePromoted() == false`.
      */
    virtual DataTypePtr promoteNumericType() const;

    /** Directly insert default value into a column. Default implementation use method IColumn::insertDefault.
      * This should be overriden if data type default value differs from column default value (example: Enum data types).
      */
    virtual void insertDefaultInto(IColumn& column) const;

    /// Checks that two instances belong to the same type
    virtual bool equals(const IDataType& rhs) const = 0;

    /// Various properties on behaviour of data type.

    /** The data type is dependent on parameters and types with different parameters are different.
      * Examples: FixedString(N), Tuple(T1, T2), Nullable(T).
      * Otherwise all instances of the same class are the same types.
      */
    virtual bool isParametric() const = 0;

    /** The data type is dependent on parameters and at least one of them is another type.
      * Examples: Tuple(T1, T2), Nullable(T). But FixedString(N) is not.
      */
    virtual bool haveSubtypes() const = 0;

    /** Can appear in table definition.
      * Counterexamples: Interval, Nothing.
      */
    virtual bool cannotBeStoredInTables() const { return false; }

    /** In text formats that render "pretty" tables,
      *  is it better to align value right in table cell.
      * Examples: numbers, even nullable.
      */
    virtual bool shouldAlignRightInPrettyFormats() const { return false; }

    /** Does formatted value in any text format can contain anything but valid UTF8 sequences.
      * Example: String (because it can contain arbitrary bytes).
      * Counterexamples: numbers, Date, DateTime.
      * For Enum, it depends.
      */
    virtual bool textCanContainOnlyValidUTF8() const { return false; }

    /** Is it possible to compare for less/greater, to calculate min/max?
      * Not necessarily totally comparable. For example, floats are comparable despite the fact that NaNs compares to nothing.
      * The same for nullable of comparable types: they are comparable (but not totally-comparable).
      */
    virtual bool isComparable() const { return false; }

    /** Does it make sense to use this type with COLLATE modifier in ORDER BY.
      * Example: String, but not FixedString.
      */
    virtual bool canBeComparedWithCollation() const { return false; }

    /** If the type is totally comparable (Ints, Date, DateTime, not nullable, not floats)
      *  and "simple" enough (not String, FixedString) to be used as version number
      *  (to select rows with maximum version).
      */
    virtual bool canBeUsedAsVersion() const { return false; }

    /** Values of data type can be summed (possibly with overflow, within the same data type).
      * Example: numbers, even nullable. Not Date/DateTime. Not Enum.
      * Enums can be passed to aggregate function 'sum', but the result is Int64, not Enum, so they are not summable.
      */
    virtual bool isSummable() const { return false; }

    /** Can be used in operations like bit and, bit shift, bit not, etc.
      */
    virtual bool canBeUsedInBitOperations() const { return false; }

    /** Can be used in boolean context (WHERE, HAVING).
      * UInt8, maybe nullable.
      */
    virtual bool canBeUsedInBooleanContext() const { return false; }

    /** Numbers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByNumber() const { return false; }

    /** Integers, Enums, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByInteger() const { return false; }

    /** Unsigned Integers, Date, DateTime. Not nullable.
      */
    virtual bool isValueRepresentedByUnsignedInteger() const { return false; }

    /** Values are unambiguously identified by contents of contiguous memory region,
      *  that can be obtained by IColumn::getDataAt method.
      * Examples: numbers, Date, DateTime, String, FixedString,
      *  and Arrays of numbers, Date, DateTime, FixedString, Enum, but not String.
      *  (because Array(String) values became ambiguous if you concatenate Strings).
      * Counterexamples: Nullable, Tuple.
      */
    virtual bool isValueUnambiguouslyRepresentedInContiguousMemoryRegion() const { return false; }

    virtual bool isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() const {
        return isValueRepresentedByNumber();
    }

    /** Example: numbers, Date, DateTime, FixedString, Enum... Nullable and Tuple of such types.
      * Counterexamples: String, Array.
      * It's Ok to return false for AggregateFunction despite the fact that some of them have fixed size state.
      */
    virtual bool haveMaximumSizeOfValue() const { return false; }

    /** Size in amount of bytes in memory. Throws an exception if not haveMaximumSizeOfValue.
      */
    virtual size_t getMaximumSizeOfValueInMemory() const { return getSizeOfValueInMemory(); }

    /** Throws an exception if value is not of fixed size.
      */
    virtual size_t getSizeOfValueInMemory() const;

    /** Integers (not floats), Enum, String, FixedString.
      */
    virtual bool isCategorial() const { return false; }

    virtual bool isNullable() const { return false; }

    /** Is this type can represent only NULL value? (It also implies isNullable)
      */
    virtual bool onlyNull() const { return false; }

    /** If this data type cannot be wrapped in Nullable data type.
      */
    virtual bool canBeInsideNullable() const { return false; }

    virtual bool lowCardinality() const { return false; }

    /// Strings, Numbers, Date, DateTime, Nullable
    virtual bool canBeInsideLowCardinality() const { return false; }

    /// Updates avg_value_size_hint for newly read column. Uses to optimize deserialization. Zero expected for first column.
    static void updateAvgValueSizeHint(const IColumn& column, double& avg_value_size_hint);

    virtual void serialize(const IColumn& column, PColumn* pcolumn) const = 0;
    virtual void deserialize(const PColumn& pcolumn, IColumn* column) const = 0;

    // static String getFileNameForStream(const String & column_name, const SubstreamPath & path);

private:
    friend class DataTypeFactory;
    /** Customize this DataType
      */
    // void setCustomization(DataTypeCustomDescPtr custom_desc_) const;

private:
    /** This is mutable to allow setting custom name and serialization on `const IDataType` post construction.
     */
    // mutable DataTypeCustomNamePtr custom_name;
    // mutable DataTypeCustomTextSerializationPtr custom_text_serialization;

public:
    // const IDataTypeCustomName * getCustomName() const { return custom_name.get(); }
};

/// Some sugar to check data type of IDataType
struct WhichDataType {
    TypeIndex idx;

    WhichDataType(TypeIndex idx_ = TypeIndex::Nothing) : idx(idx_) {}

    WhichDataType(const IDataType& data_type) : idx(data_type.getTypeId()) {}

    WhichDataType(const IDataType* data_type) : idx(data_type->getTypeId()) {}

    WhichDataType(const DataTypePtr& data_type) : idx(data_type->getTypeId()) {}

    bool isUInt8() const { return idx == TypeIndex::UInt8; }
    bool isUInt16() const { return idx == TypeIndex::UInt16; }
    bool isUInt32() const { return idx == TypeIndex::UInt32; }
    bool isUInt64() const { return idx == TypeIndex::UInt64; }
    bool isUInt128() const { return idx == TypeIndex::UInt128; }
    bool isUInt() const {
        return isUInt8() || isUInt16() || isUInt32() || isUInt64() || isUInt128();
    }
    bool isNativeUInt() const { return isUInt8() || isUInt16() || isUInt32() || isUInt64(); }

    bool isInt8() const { return idx == TypeIndex::Int8; }
    bool isInt16() const { return idx == TypeIndex::Int16; }
    bool isInt32() const { return idx == TypeIndex::Int32; }
    bool isInt64() const { return idx == TypeIndex::Int64; }
    bool isInt128() const { return idx == TypeIndex::Int128; }
    bool isInt() const { return isInt8() || isInt16() || isInt32() || isInt64() || isInt128(); }
    bool isNativeInt() const { return isInt8() || isInt16() || isInt32() || isInt64(); }

    bool isDecimal32() const { return idx == TypeIndex::Decimal32; }
    bool isDecimal64() const { return idx == TypeIndex::Decimal64; }
    bool isDecimal128() const { return idx == TypeIndex::Decimal128; }
    bool isDecimal() const { return isDecimal32() || isDecimal64() || isDecimal128(); }

    bool isFloat32() const { return idx == TypeIndex::Float32; }
    bool isFloat64() const { return idx == TypeIndex::Float64; }
    bool isFloat() const { return isFloat32() || isFloat64(); }

    bool isEnum8() const { return idx == TypeIndex::Enum8; }
    bool isEnum16() const { return idx == TypeIndex::Enum16; }
    bool isEnum() const { return isEnum8() || isEnum16(); }

    bool isDate() const { return idx == TypeIndex::Date; }
    bool isDateTime() const { return idx == TypeIndex::DateTime; }
    bool isDateOrDateTime() const { return isDate() || isDateTime(); }

    bool isString() const { return idx == TypeIndex::String; }
    bool isFixedString() const { return idx == TypeIndex::FixedString; }
    bool isStringOrFixedString() const { return isString() || isFixedString(); }

    bool isUUID() const { return idx == TypeIndex::UUID; }
    bool isArray() const { return idx == TypeIndex::Array; }
    bool isTuple() const { return idx == TypeIndex::Tuple; }
    bool isSet() const { return idx == TypeIndex::Set; }
    bool isInterval() const { return idx == TypeIndex::Interval; }

    bool isNothing() const { return idx == TypeIndex::Nothing; }
    bool isNullable() const { return idx == TypeIndex::Nullable; }
    bool isFunction() const { return idx == TypeIndex::Function; }
    bool isAggregateFunction() const { return idx == TypeIndex::AggregateFunction; }
};

/// IDataType helpers (alternative for IDataType virtual methods with single point of truth)

inline bool isDate(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isDate();
}
inline bool isDateOrDateTime(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isDateOrDateTime();
}
inline bool isEnum(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isEnum();
}
inline bool isDecimal(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isDecimal();
}
inline bool isTuple(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isTuple();
}
inline bool isArray(const DataTypePtr& data_type) {
    return WhichDataType(data_type).isArray();
}

template <typename T>
inline bool isUInt8(const T& data_type) {
    return WhichDataType(data_type).isUInt8();
}

template <typename T>
inline bool isUnsignedInteger(const T& data_type) {
    return WhichDataType(data_type).isUInt();
}

template <typename T>
inline bool isInteger(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

template <typename T>
inline bool isFloat(const T& data_type) {
    WhichDataType which(data_type);
    return which.isFloat();
}

template <typename T>
inline bool isNativeNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isNativeInt() || which.isNativeUInt() || which.isFloat();
}

template <typename T>
inline bool isNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDecimal();
}

template <typename T>
inline bool isColumnedAsNumber(const T& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt() || which.isFloat() || which.isDateOrDateTime() ||
           which.isUUID();
}

template <typename T>
inline bool isString(const T& data_type) {
    return WhichDataType(data_type).isString();
}

template <typename T>
inline bool isFixedString(const T& data_type) {
    return WhichDataType(data_type).isFixedString();
}

template <typename T>
inline bool isStringOrFixedString(const T& data_type) {
    return WhichDataType(data_type).isStringOrFixedString();
}

inline bool isNotDecimalButComparableToDecimal(const DataTypePtr& data_type) {
    WhichDataType which(data_type);
    return which.isInt() || which.isUInt();
}

inline bool isCompilableType(const DataTypePtr& data_type) {
    return data_type->isValueRepresentedByNumber() && !isDecimal(data_type);
}

} // namespace vectorized
} // namespace doris
