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

#include "runtime/string_value.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/**
 * used to keep predicate column in storage layer
 * 
 *  T = predicate column type
 */
template <typename T>
class PredicateColumnType final : public COWHelper<IColumn, PredicateColumnType<T>> {
private:
    PredicateColumnType() {}
    PredicateColumnType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, PredicateColumnType<T>>;

    PredicateColumnType(const PredicateColumnType& src) : data(src.data.begin(), src.data.end()) {}

public:
    using Self = PredicateColumnType;
    using value_type = T;
    using Container = PaddedPODArray<value_type>;

    bool is_numeric() const override { return false; }

    bool is_complex_column() const override { return true; }

    size_t size() const override { return data.size(); }

   [[noreturn]]  StringRef get_data_at(size_t n) const override {
         LOG(FATAL) << "get_data_at not supported in PredicateColumnType";
    }

    void insert_from(const IColumn& src, size_t n) override {
         LOG(FATAL) << "insert_from not supported in PredicateColumnType";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
         LOG(FATAL) << "insert_range_from not supported in PredicateColumnType";
    }

    void pop_back(size_t n) {
        LOG(FATAL) << "pop_back not supported in PredicateColumnType";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const {
         LOG(FATAL) << "update_hash_with_value not supported in PredicateColumnType";
    }

    void insert_string_value(char* data_ptr, size_t length) {
        StringValue sv(data_ptr, length);
        data.push_back_without_reserve(sv);
    }

    void insert_decimal_value(char* data_ptr, size_t length) {
        decimal12_t dc12_value;
        dc12_value.integer = *(int64_t*)(data_ptr);
        dc12_value.fraction = *(int32_t*)(data_ptr + sizeof(int64_t));
        data.push_back_without_reserve(dc12_value);
    }

    // used for int128
    void insert_in_copy_way(char* data_ptr, size_t length) {
        T val {};
        memcpy(&val, data_ptr, sizeof(val));
        data.push_back_without_reserve(val);
    }
    
    void insert_default_type(char* data_ptr, size_t length) {
        T* val = (T*)data_ptr;
        data.push_back_without_reserve(*val);
    }

    void insert_data(const char* data_ptr, size_t length) override {
        char* ch = const_cast<char*>(data_ptr);
        if constexpr (std::is_same_v<T, StringValue>) {
            insert_string_value(ch, length);
         } else if constexpr (std::is_same_v<T, decimal12_t>) {
            insert_decimal_value(ch, length);
         } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            insert_in_copy_way(ch, length);
         } else {
            insert_default_type(ch, length);
         }
    }

    void insert_default() override { 
        data.push_back(T()); 
    }

    void clear() override { data.clear(); }

    size_t byte_size() const override { 
         return data.size() * sizeof(T);
    }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not supported in PredicateColumnType";
    }

    void reserve(size_t n) override { 
        data.reserve(n); 
    }

    [[noreturn]] const char* get_family_name() const override { 
        LOG(FATAL) << "get_family_name not supported in PredicateColumnType";
    }

   [[noreturn]] MutableColumnPtr clone_resized(size_t size) const override {
        LOG(FATAL) << "clone_resized not supported in PredicateColumnType";
    }

    void insert(const Field& x) override {
        LOG(FATAL) << "insert not supported in PredicateColumnType";
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not supported in PredicateColumnType";
    }

    void get(size_t n, Field& res) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnTyped";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not supported in PredicateColumnType";
    }

    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
        LOG(FATAL) << "serialize_value_into_arena not supported in PredicateColumnType";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) {
        LOG(FATAL) << "deserialize_and_insert_from_arena not supported in PredicateColumnType";
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const {
        LOG(FATAL) << "compare_at not supported in PredicateColumnType";
    }

    void get_extremes(Field& min, Field& max) const {
        LOG(FATAL) << "get_extremes not supported in PredicateColumnType";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    [[noreturn]] StringRef get_raw_data() const override {
        LOG(FATAL) << "get_raw_data not supported in PredicateColumnType";
    }

    [[noreturn]] bool structure_equals(const IColumn& rhs) const override {
         LOG(FATAL) << "structure_equals not supported in PredicateColumnType";
    }

    [[noreturn]] ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override {
         LOG(FATAL) << "filter not supported in PredicateColumnType";
    };

    [[noreturn]] ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override { 
         LOG(FATAL) << "permute not supported in PredicateColumnType";
    };

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    [[noreturn]] ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override {
        LOG(FATAL) << "replicate not supported in PredicateColumnType";
    };

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not supported in PredicateColumnType";
    }

    ColumnPtr filter_decimal_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnDecimal<Decimal128>::create(0, 9); // todo(wb) need a global variable to stand for scale
            if (sel_size == 0) {
                return res;
            }
            for (size_t i = 0; i < sel_size; i++) {
                uint16_t n = sel[i];
                auto& dv = reinterpret_cast<const decimal12_t&>(data[n]);
                DecimalV2Value dv_data(dv.integer, dv.fraction);
                res->insert_data(reinterpret_cast<char*>(&dv_data), 0);
            }
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr res_ptr = (*std::move(*ptr)).assume_mutable();

                for (size_t i = 0; i < sel_size; i++) {
                    uint16_t n = sel[i];
                    auto& dv = reinterpret_cast<const decimal12_t&>(data[n]);
                    DecimalV2Value dv_data(dv.integer, dv.fraction);
                    res_ptr->insert_data(reinterpret_cast<char*>(&dv_data), 0);
                }
            }
        }
        return nullptr;
    }

    ColumnPtr filter_date_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnVector<Int128>::create();
            if (sel_size == 0) {
                return res;
            }

            const T* data_pos = data.data();
            for (size_t i = 0; i < sel_size; i++) {
                const T val = data_pos[sel[i]];
                const char* val_ptr = reinterpret_cast<const char*>(&val);
        
                uint64_t value = 0;
                value = *(unsigned char*)(val_ptr + 2);
                value <<= 8;
                value |= *(unsigned char*)(val_ptr + 1);
                value <<= 8;
                value |= *(unsigned char*)(val_ptr);
                DateTimeValue date;
                date.from_olap_date(value);
        
                res->insert_data(reinterpret_cast<char*>(&date), 0);
            }
            return res;
        } else {
            if (sel_size == 0) {
                return nullptr;
            }  

            const T* data_pos = data.data();
            for (size_t i = 0; i < sel_size; i++) {
                const T val = data_pos[sel[i]];
                const char* val_ptr = reinterpret_cast<const char*>(&val);
        
                uint64_t value = 0;
                value = *(unsigned char*)(val_ptr + 2);
                value <<= 8;
                value |= *(unsigned char*)(val_ptr + 1);
                value <<= 8;
                value |= *(unsigned char*)(val_ptr);
                DateTimeValue date;
                date.from_olap_date(value);
        
                (*std::move(*ptr)).assume_mutable()->insert_data(reinterpret_cast<char*>(&date), 0);
            }
        }
        return nullptr;
    }

    ColumnPtr filter_string_value_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            auto res = vectorized::ColumnString::create();
            if (sel_size == 0) {
                return res;
            }
            res->reserve(sel_size);
            for (size_t i = 0; i < sel_size; i++) {
                uint16_t n = sel[i];
                auto& sv = reinterpret_cast<StringValue&>(data[n]);
                res->insert_data(sv.ptr, sv.len);
            }
            return res;
        } else {
            if (sel_size != 0) {
                MutableColumnPtr ptr_res = (*std::move(*ptr)).assume_mutable();

                for (size_t i = 0; i < sel_size; i++) {
                    uint16_t n = sel[i];
                    auto& sv = reinterpret_cast<StringValue&>(data[n]);
                    ptr_res->insert_data(sv.ptr, sv.len);
                }
            }
        }
        return nullptr;
    }

    template <typename Y>
    ColumnPtr filter_default_type_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) {
        if (ptr == nullptr) {
            MutableColumnPtr res_ptr = vectorized::ColumnVector<Y>::create();
            if (sel_size == 0) {
                return res_ptr;
            }

            res_ptr->reserve(sel_size);
            for (size_t i = 0; i < sel_size; i++) {
                T* val_ptr = &data[sel[i]];
                res_ptr->insert_data((char*)val_ptr, 0);
            }
            return res_ptr;
        } else {
            if (sel_size == 0) {
                return nullptr;
            }
            MutableColumnPtr ptr_res = (*std::move(*ptr)).assume_mutable();
            for (size_t i = 0; i < sel_size; i++) {
                T* val_ptr = &data[sel[i]];
                ptr_res->insert_data((char*)val_ptr, 0);
            }
        }
        return nullptr;
    }


    ColumnPtr filter_by_selector(const uint16_t* sel, size_t sel_size, ColumnPtr* ptr = nullptr) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            return filter_string_value_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, decimal12_t>) {
            return filter_decimal_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int8>) {
            return filter_default_type_by_selector<doris::vectorized::Int8>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int16>) {
            return filter_default_type_by_selector<doris::vectorized::Int16>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int32>) {
            return filter_default_type_by_selector<doris::vectorized::Int32>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int64>) {
            return filter_default_type_by_selector<doris::vectorized::Int64>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float32>) {
            return filter_default_type_by_selector<doris::vectorized::Float32>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Float64>) {
            return filter_default_type_by_selector<doris::vectorized::Float64>(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            return filter_date_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, uint24_t>) {
            return filter_date_by_selector(sel, sel_size, ptr);
        } else if constexpr (std::is_same_v<T, doris::vectorized::Int128>) {
            return filter_default_type_by_selector<doris::vectorized::Int128>(sel, sel_size, ptr);
        } else {
            return filter_default_type_by_selector<T>(sel, sel_size, ptr);
        }
    }

private:
    Container data;
};
using ColumnStringValue = PredicateColumnType<StringValue>;


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

    bool is_numeric() const override { return false; }

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        return StringRef(reinterpret_cast<const char*>(&data[n]), sizeof(data[n]));
    }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(static_cast<const Self&>(src).get_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        data.push_back(*reinterpret_cast<const T*>(pos));
    }

    void insert_default() override { data.push_back(T()); }

    void clear() override { data.clear(); }

    // TODO: value_type is not a pod type, so we also need to
    // calculate the memory requested by value_type
    size_t byte_size() const override { return data.size() * sizeof(data[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    void protect() override {}

    void insert_value(T value) { data.emplace_back(std::move(value)); }

    [[noreturn]] void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                      IColumn::Permutation& res) const override {
        LOG(FATAL) << "get_permutation not implemented";
    }

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override { data.resize(n); }

    const char* get_family_name() const override { return TypeName<T>::get(); }

    MutableColumnPtr clone_resized(size_t size) const override;

    [[noreturn]] void insert(const Field& x) override {
        LOG(FATAL) << "insert field not implemented";
    }

    [[noreturn]] Field operator[](size_t n) const override {
        LOG(FATAL) << "operator[] not implemented";
    }
    [[noreturn]] void get(size_t n, Field& res) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] UInt64 get64(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] Float64 get_float64(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] UInt64 get_uint(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        LOG(FATAL) << "get field not implemented";
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) {
        auto& col = static_cast<const Self&>(src);
        auto& src_data = col.get_data();
        auto st = src_data.begin() + start;
        auto ed = st + length;
        data.insert(data.end(), st, ed);
    }

    void pop_back(size_t n) { data.erase(data.end() - n, data.end()); }
    // it's impossable to use ComplexType as key , so we don't have to implemnt them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const {
        LOG(FATAL) << "serialize_value_into_arena not implemented";
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) {
        LOG(FATAL) << "deserialize_and_insert_from_arena not implemented";
    }

    void update_hash_with_value(size_t n, SipHash& hash) const {
        // TODO add hash function
    }

    [[noreturn]] int compare_at(size_t n, size_t m, const IColumn& rhs,
                                int nan_direction_hint) const {
        LOG(FATAL) << "compare_at not implemented";
    }

    void get_extremes(Field& min, Field& max) const {
        LOG(FATAL) << "get_extremes not implemented";
    }

    bool can_be_inside_nullable() const override { return true; }

    bool is_fixed_and_contiguous() const override { return true; }
    size_t size_of_value_if_fixed() const override { return sizeof(T); }

    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnComplexType<T>);
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    const T& get_element(size_t n) const { return data[n]; }

    T& get_element(size_t n) { return data[n]; }

    ColumnPtr replicate(const IColumn::Offsets& replicate_offsets) const override;

    [[noreturn]] MutableColumns scatter(IColumn::ColumnIndex num_columns,
                                        const IColumn::Selector& selector) const override {
        LOG(FATAL) << "scatter not implemented";
    }

    void replace_column_data(const IColumn& rhs, size_t row) override {
        DCHECK(size() == 1);
        data[0] = static_cast<const Self&>(rhs).data[row];
    }

private:
    Container data;
};

template <typename T>
MutableColumnPtr ColumnComplexType<T>::clone_resized(size_t size) const {
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
    if (size != filt.size()) {
        LOG(FATAL) << "Size of filter doesn't match size of column.";
    }

    if (data.size() == 0) return this->create();
    auto res = this->create();
    Container& res_data = res->get_data();

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

    if (perm.size() < limit) {
        LOG(FATAL) << "Size of permutation is less than required.";
    }

    auto res = this->create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) {
        res_data[i] = data[perm[i]];
    }

    return res;
}

template <typename T>
ColumnPtr ColumnComplexType<T>::replicate(const IColumn::Offsets& offsets) const {
    size_t size = data.size();
    if (size != offsets.size()) {
        LOG(FATAL) << "Size of offsets doesn't match size of column.";
    }

    if (0 == size) return this->create();

    auto res = this->create();
    typename Self::Container& res_data = res->get_data();
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

using ColumnBitmap = ColumnComplexType<BitmapValue>;
} // namespace doris::vectorized