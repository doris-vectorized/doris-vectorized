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

#include <vector>

#include "util/bitmap_value.h"
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"

namespace doris::vectorized {
template <typename T>
class ColumnComplexType final : public COWHelper<IColumn, ColumnComplexType<T>> {
private:
    ColumnComplexType() {}
    ColumnComplexType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, ColumnComplexType<T>>;

public:
    using Self = ColumnComplexType;
    using value_type = T;
    using Container = std::vector<value_type>;

    bool isNumeric() const override { return false; }

    size_t size() const override { return data.size(); }

    StringRef getDataAt(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&data[n]), sizeof(data[n]));
    }

    void insertFrom(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const Self&>(src).getData()[n]);
    }

    void insertData(const char* pos, size_t /*length*/) override {
        data.push_back(*reinterpret_cast<const T*>(pos));
    }

    void insertDefault() override { data.push_back(T()); }

    // TODO: value_type is not a pod type, so we also need to
    // calculate the memory requested by value_type
    size_t byteSize() const override { return data.size() * sizeof(data[0]); }

    size_t allocatedBytes() const override { return byteSize(); }

    void protect() override {}

    void insertValue(T value) { data.emplace_back(std::move(value)); }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint,
                        IColumn::Permutation& res) const override {
        throw Exception("getPermutation not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void reserve(size_t n) override { data.reserve(n); }

    const char* getFamilyName() const override { return TypeName<T>::get(); }

    MutableColumnPtr cloneResized(size_t size) const override;

    void insert(const Field& x) override {
        throw Exception("insert field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    Field operator[](size_t n) const override {
        throw Exception("operator[] not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }
    void get(size_t n, Field& res) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    UInt64 get64(size_t n) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    Float64 getFloat64(size_t n) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    UInt64 getUInt(size_t n) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool getBool(size_t n) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    Int64 getInt(size_t n) const override {
        throw Exception("get field not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertRangeFrom(const IColumn& src, size_t start, size_t length) {
        auto& col = static_cast<const Self&>(src);
        auto& src_data = col.getData();
        auto st = src_data.begin() + start;
        auto ed = st + length;
        data.insert(data.end(), st, ed);
    }

    void popBack(size_t n) { data.erase(data.end() - n, data.end()); }
    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    StringRef serializeValueIntoArena(size_t n, Arena& arena, char const*& begin) const {
        throw Exception("serializeValueIntoArena not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    const char* deserializeAndInsertFromArena(const char* pos) {
        throw Exception("deserializeAndInsertFromArena not implemented",
                        ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValue(size_t n, SipHash& hash) const {
        // TODO add hash function
    }

    int compareAt(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const {
        throw Exception("compareAt not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field& min, Field& max) const {
        throw Exception("getExtremes not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return sizeof(T); }

    StringRef getRawData() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structureEquals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnComplexType<T>);
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    Container& getData() { return data; }

    const Container& getData() const { return data; }

    const T& getElement(size_t n) const { return data[n]; }

    T& getElement(size_t n) { return data[n]; }

    ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override;

    MutableColumns scatter(IColumn::ColumnIndex num_columns,
                           const IColumn::Selector& selector) const override {
        throw Exception("scatter not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    Container data;
};

template <typename T>
MutableColumnPtr ColumnComplexType<T>::cloneResized(size_t size) const {
    auto res = this->create();

    if (size > 0) {
        auto& new_col = static_cast<Self&>(*res);
        new_col.data = this->data;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::filter(const IColumn::Filter& filt,
                                       ssize_t result_size_hint) const {
    size_t size = data.size();
    if (size != filt.size())
        throw Exception("Size of filter doesn't match size of column.",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    if (data.size() == 0) return this->create();
    auto res = this->create();
    Container& res_data = res->getData();

    if (result_size_hint) res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const T* data_pos = data.data();

    while (filt_pos < filt_end) {
        if (*filt_pos) res_data.push_back(*data_pos);

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::permute(const IColumn::Permutation& perm, size_t limit) const {
    size_t size = data.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = this->create(limit);
    typename Self::Container& res_data = res->getData();
    for (size_t i = 0; i < limit; ++i) {
        res_data[i] = data[perm[i]];
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    if (size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (0 == size) return this->create();

    auto res = this->create();
    typename Self::Container& res_data = res->getData();
    res_data.reserve(offsets.back());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0; i < size; ++i) {
        size_t size_to_replicate = offsets[i] - prev_offset;
        prev_offset = offsets[i];

        for (size_t j = 0; j < size_to_replicate; ++j) {
            res_data.push_back(data[i]);
        }
    }

    return res;
}

using ColumnBitMap = ColumnComplexType<BitmapValue>;
} // namespace doris::vectorized