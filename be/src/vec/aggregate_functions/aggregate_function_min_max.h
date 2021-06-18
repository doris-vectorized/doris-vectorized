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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/// For numeric values.
template <typename T>
struct SingleValueDataFixed {
private:
    using Self = SingleValueDataFixed;

    bool has_value =
            false; /// We need to remember if at least one value has been passed. This is necessary for AggregateFunctionIf.
    T value;

public:
    bool has() const { return has_value; }

    void insertResultInto(IColumn& to) const {
        if (has())
            assert_cast<ColumnVector<T>&>(to).get_data().push_back(value);
        else
            assert_cast<ColumnVector<T>&>(to).insert_default();
    }

    void write(std::ostream& buf) const {
        writeBinary(has(), buf);
        if (has()) write_binary(value, buf);
    }

    void read(std::istream& buf) {
        read_binary(has_value, buf);
        if (has()) read_binary(value, buf);
    }

    void change(const IColumn& column, size_t row_num, Arena*) {
        has_value = true;
        value = assert_cast<const ColumnVector<T>&>(column).get_data()[row_num];
    }

    /// Assuming to.has()
    void change(const Self& to, Arena*) {
        has_value = true;
        value = to.value;
    }

    bool changeFirstTime(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has()) {
            change(column, row_num, arena);
            return true;
        } else
            return false;
    }

    bool changeFirstTime(const Self& to, Arena* arena) {
        if (!has() && to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeEveryTime(const IColumn& column, size_t row_num, Arena* arena) {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self& to, Arena* arena) {
        if (to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeIfLess(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnVector<T>&>(column).get_data()[row_num] < value) {
            change(column, row_num, arena);
            return true;
        } else
            return false;
    }

    bool changeIfLess(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeIfGreater(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has() || assert_cast<const ColumnVector<T>&>(column).get_data()[row_num] > value) {
            change(column, row_num, arena);
            return true;
        } else
            return false;
    }

    bool changeIfGreater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool isEqualTo(const Self& to) const { return has() && to.value == value; }

    bool isEqualTo(const IColumn& column, size_t row_num) const {
        return has() && assert_cast<const ColumnVector<T>&>(column).get_data()[row_num] == value;
    }
};

/** For strings. Short strings are stored in the object itself, and long strings are allocated separately.
  * NOTE It could also be suitable for arrays of numbers.
  */
struct SingleValueDataString {
private:
    using Self = SingleValueDataString;

    Int32 size = -1;    /// -1 indicates that there is no value.
    Int32 capacity = 0; /// power of two or zero
    char* large_data;

public:
    static constexpr Int32 AUTOMATIC_STORAGE_SIZE = 64;
    static constexpr Int32 MAX_SMALL_STRING_SIZE =
            AUTOMATIC_STORAGE_SIZE - sizeof(size) - sizeof(capacity) - sizeof(large_data);

private:
    char small_data[MAX_SMALL_STRING_SIZE]; /// Including the terminating zero.

public:
    bool has() const { return size >= 0; }

    const char* get_data() const { return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data; }

    StringRef getStringRef() const { return StringRef(get_data(), size); }

    // void insertResultInto(IColumn & to) const
    // {
    //     if (has())
    //         assert_cast<ColumnString &>(to).insert_data_with_terminating_zero(get_data(), size);
    //     else
    //         assert_cast<ColumnString &>(to).insert_default();
    // }

    // void write(WriteBuffer & buf, const IDataType & /*data_type*/) const
    // {
    //     write_binary(size, buf);
    //     if (has())
    //         buf.write(get_data(), size);
    // }

    // void read(ReadBuffer & buf, const IDataType & /*data_type*/, Arena * arena)
    // {
    //     Int32 rhs_size;
    //     read_binary(rhs_size, buf);

    //     if (rhs_size >= 0)
    //     {
    //         if (rhs_size <= MAX_SMALL_STRING_SIZE)
    //         {
    //             /// Don't free large_data here.

    //             size = rhs_size;

    //             if (size > 0)
    //                 buf.read(small_data, size);
    //         }
    //         else
    //         {
    //             if (capacity < rhs_size)
    //             {
    //                 capacity = static_cast<UInt32>(roundUpToPowerOfTwoOrZero(rhs_size));
    //                 /// Don't free large_data here.
    //                 large_data = arena->alloc(capacity);
    //             }

    //             size = rhs_size;
    //             buf.read(large_data, size);
    //         }
    //     }
    //     else
    //     {
    //         /// Don't free large_data here.
    //         size = rhs_size;
    //     }
    // }

    /// Assuming to.has()
    void changeImpl(StringRef value, Arena* arena) {
        // Int32 value_size = value.size;

        // if (value_size <= MAX_SMALL_STRING_SIZE)
        // {
        //     /// Don't free large_data here.
        //     size = value_size;

        //     if (size > 0)
        //         memcpy(small_data, value.data, size);
        // }
        // else
        // {
        //     if (capacity < value_size)
        //     {
        //         /// Don't free large_data here.
        //         capacity = roundUpToPowerOfTwoOrZero(value_size);
        //         large_data = arena->alloc(capacity);
        //     }

        //     size = value_size;
        //     memcpy(large_data, value.data, size);
        // }
    }

    void change(const IColumn& column, size_t row_num, Arena* arena) {
        // changeImpl(assert_cast<const ColumnString &>(column).get_data_at_with_terminating_zero(row_num), arena);
    }

    void change(const Self& to, Arena* arena) { changeImpl(to.getStringRef(), arena); }

    bool changeFirstTime(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has()) {
            change(column, row_num, arena);
            return true;
        } else
            return false;
    }

    bool changeFirstTime(const Self& to, Arena* arena) {
        if (!has() && to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeEveryTime(const IColumn& column, size_t row_num, Arena* arena) {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self& to, Arena* arena) {
        if (to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    // bool changeIfLess(const IColumn & column, size_t row_num, Arena * arena)
    // {
    //     if (!has() || assert_cast<const ColumnString &>(column).get_data_at_with_terminating_zero(row_num) < getStringRef())
    //     {
    //         change(column, row_num, arena);
    //         return true;
    //     }
    //     else
    //         return false;
    // }

    bool changeIfLess(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.getStringRef() < getStringRef())) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    // bool changeIfGreater(const IColumn & column, size_t row_num, Arena * arena)
    // {
    //     if (!has() || assert_cast<const ColumnString &>(column).get_data_at_with_terminating_zero(row_num) > getStringRef())
    //     {
    //         change(column, row_num, arena);
    //         return true;
    //     }
    //     else
    //         return false;
    // }

    bool changeIfGreater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.getStringRef() > getStringRef())) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool isEqualTo(const Self& to) const { return has() && to.getStringRef() == getStringRef(); }

    bool isEqualTo(const IColumn& column, size_t row_num) const {
        // return has() && assert_cast<const ColumnString &>(column).get_data_at_with_terminating_zero(row_num) == getStringRef();
        return false;
    }
};

/// For any other value types.
struct SingleValueDataGeneric {
private:
    using Self = SingleValueDataGeneric;

    Field value;

public:
    bool has() const { return !value.isNull(); }

    void insertResultInto(IColumn& to) const {
        if (has())
            to.insert(value);
        else
            to.insert_default();
    }

    // void write(WriteBuffer & buf, const IDataType & data_type) const
    // {
    //     if (!value.isNull())
    //     {
    //         write_binary(true, buf);
    //         data_type.serializeBinary(value, buf);
    //     }
    //     else
    //         write_binary(false, buf);
    // }

    // void read(ReadBuffer & buf, const IDataType & data_type, Arena *)
    // {
    //     bool is_not_null;
    //     read_binary(is_not_null, buf);

    //     if (is_not_null)
    //         data_type.deserializeBinary(value, buf);
    // }

    void change(const IColumn& column, size_t row_num, Arena*) { column.get(row_num, value); }

    void change(const Self& to, Arena*) { value = to.value; }

    bool changeFirstTime(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has()) {
            change(column, row_num, arena);
            return true;
        } else
            return false;
    }

    bool changeFirstTime(const Self& to, Arena* arena) {
        if (!has() && to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeEveryTime(const IColumn& column, size_t row_num, Arena* arena) {
        change(column, row_num, arena);
        return true;
    }

    bool changeEveryTime(const Self& to, Arena* arena) {
        if (to.has()) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeIfLess(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has()) {
            change(column, row_num, arena);
            return true;
        } else {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value < value) {
                value = new_value;
                return true;
            } else
                return false;
        }
    }

    bool changeIfLess(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value < value)) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool changeIfGreater(const IColumn& column, size_t row_num, Arena* arena) {
        if (!has()) {
            change(column, row_num, arena);
            return true;
        } else {
            Field new_value;
            column.get(row_num, new_value);
            if (new_value > value) {
                value = new_value;
                return true;
            } else
                return false;
        }
    }

    bool changeIfGreater(const Self& to, Arena* arena) {
        if (to.has() && (!has() || to.value > value)) {
            change(to, arena);
            return true;
        } else
            return false;
    }

    bool isEqualTo(const IColumn& column, size_t row_num) const {
        return has() && value == column[row_num];
    }

    bool isEqualTo(const Self& to) const { return has() && to.value == value; }
};

template <typename Data>
struct AggregateFunctionMaxData : Data {
    using Self = AggregateFunctionMaxData;

    bool changeIfBetter(const IColumn& column, size_t row_num, Arena* arena) {
        return this->changeIfGreater(column, row_num, arena);
    }
    bool changeIfBetter(const Self& to, Arena* arena) { return this->changeIfGreater(to, arena); }

    static const char* name() { return "max"; }
};

template <typename Data>
struct AggregateFunctionMinData : Data {
    using Self = AggregateFunctionMinData;

    bool changeIfBetter(const IColumn& column, size_t row_num, Arena* arena) {
        return this->changeIfLess(column, row_num, arena);
    }
    bool changeIfBetter(const Self& to, Arena* arena) { return this->changeIfLess(to, arena); }

    static const char* name() { return "min"; }
};

template <typename Data, bool AllocatesMemoryInArena>
class AggregateFunctionsSingleValue final
        : public IAggregateFunctionDataHelper<
                  Data, AggregateFunctionsSingleValue<Data, AllocatesMemoryInArena>> {
private:
    DataTypePtr& type;

public:
    AggregateFunctionsSingleValue(const DataTypePtr& type_)
            : IAggregateFunctionDataHelper<
                      Data, AggregateFunctionsSingleValue<Data, AllocatesMemoryInArena>>({type_},
                                                                                         {}),
              type(this->argument_types[0]) {
        if (StringRef(Data::name()) == StringRef("min") ||
            StringRef(Data::name()) == StringRef("max")) {
            if (!type->isComparable())
                throw Exception("Illegal type " + type->get_name() +
                                        " of argument of aggregate function " + get_name() +
                                        " because the values of that data type are not comparable",
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr getReturnType() const override { return type; }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).changeIfBetter(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena* arena) const override {
        this->data(place).changeIfBetter(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena*) const override {
        this->data(place).read(buf);
    }

    bool allocatesMemoryInArena() const override { return AllocatesMemoryInArena; }

    void insertResultInto(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insertResultInto(to);
    }

    const char* getHeaderFilePath() const override { return __FILE__; }
};

} // namespace doris::vectorized