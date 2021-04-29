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

#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
class ColumnNullable final : public COWHelper<IColumn, ColumnNullable> {
private:
    friend class COWHelper<IColumn, ColumnNullable>;

    ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_);
    ColumnNullable(const ColumnNullable&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnNullable>;
    static Ptr create(const ColumnPtr& nested_column_, const ColumnPtr& null_map_) {
        return ColumnNullable::create(nested_column_->assumeMutable(), null_map_->assumeMutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    const char* getFamilyName() const override { return "Nullable"; }
    std::string getName() const override { return "Nullable(" + nested_column->getName() + ")"; }
    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override { return nested_column->size(); }
    bool isNullAt(size_t n) const override {
        return assert_cast<const ColumnUInt8&>(*null_map).getData()[n] != 0;
    }
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool getBool(size_t n) const override { return isNullAt(n) ? 0 : nested_column->getBool(n); }
    UInt64 get64(size_t n) const override { return nested_column->get64(n); }
    StringRef getDataAt(size_t n) const override;

    /// Will insert null value if pos=nullptr
    void insertData(const char* pos, size_t length) override;
    StringRef serializeValueIntoArena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserializeAndInsertFromArena(const char* pos) override;
    void insertRangeFrom(const IColumn& src, size_t start, size_t length) override;
    void insert(const Field& x) override;
    void insertFrom(const IColumn& src, size_t n) override;

    void insertFromNotNullable(const IColumn& src, size_t n);
    void insertRangeFromNotNullable(const IColumn& src, size_t start, size_t length);
    void insertManyFromNotNullable(const IColumn& src, size_t position, size_t length);

    void insertDefault() override {
        getNestedColumn().insertDefault();
        getNullMapData().push_back(1);
    }

    void popBack(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compareAt(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;
    void getPermutation(bool reverse, size_t limit, int null_direction_hint,
                        Permutation& res) const override;
    void reserve(size_t n) override;
    size_t byteSize() const override;
    size_t allocatedBytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets& replicate_offsets) const override;
    void updateHashWithValue(size_t n, SipHash& hash) const override;
    void getExtremes(Field& min, Field& max) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override {
        return scatterImpl<ColumnNullable>(num_columns, selector);
    }

    //    void gather(ColumnGathererStream & gatherer_stream) override;

    void forEachSubcolumn(ColumnCallback callback) override {
        callback(nested_column);
        callback(null_map);
    }

    bool structureEquals(const IColumn& rhs) const override {
        if (auto rhs_nullable = typeid_cast<const ColumnNullable*>(&rhs))
            return nested_column->structureEquals(*rhs_nullable->nested_column);
        return false;
    }

    bool isNullable() const override { return true; }
    bool isFixedAndContiguous() const override { return false; }
    bool valuesHaveFixedSize() const override { return nested_column->valuesHaveFixedSize(); }
    size_t sizeOfValueIfFixed() const override {
        return null_map->sizeOfValueIfFixed() + nested_column->sizeOfValueIfFixed();
    }
    bool onlyNull() const override { return nested_column->isDummy(); }

    /// Return the column that represents values.
    IColumn& getNestedColumn() { return *nested_column; }
    const IColumn& getNestedColumn() const { return *nested_column; }

    const ColumnPtr& getNestedColumnPtr() const { return nested_column; }

    /// Return the column that represents the byte map.
    const ColumnPtr& getNullMapColumnPtr() const { return null_map; }

    ColumnUInt8& getNullMapColumn() { return assert_cast<ColumnUInt8&>(*null_map); }
    const ColumnUInt8& getNullMapColumn() const {
        return assert_cast<const ColumnUInt8&>(*null_map);
    }

    NullMap& getNullMapData() { return getNullMapColumn().getData(); }
    const NullMap& getNullMapData() const { return getNullMapColumn().getData(); }

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void applyNullMap(const ColumnNullable& other);
    void applyNullMap(const ColumnUInt8& map);
    void applyNegatedNullMap(const ColumnUInt8& map);

    /// Check that size of null map equals to size of nested column.
    void checkConsistency() const;

    bool has_null() const {
        size_t size = getNullMapData().size();
        const UInt8* null_pos = getNullMapData().data();
        const UInt8* null_pos_end = getNullMapData().data() + size;
 #ifdef __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */
        static constexpr size_t SIMD_BYTES = 16;
        const __m128i zero16 = _mm_setzero_si128();
        const UInt8* null_end_sse = null_pos + size / SIMD_BYTES * SIMD_BYTES;

        while (null_pos < null_end_sse) {
            int mask = _mm_movemask_epi8(_mm_cmpgt_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(null_pos)), zero16));

            if (0 != mask) {
                return true;
            }
            null_pos += SIMD_BYTES;
        }
#endif
        while (null_pos < null_pos_end) {
            if (*null_pos != 0) {
                return true;
            }
            null_pos++;
        }
        return false;
    }

private:
    WrappedPtr nested_column;
    WrappedPtr null_map;

    template <bool negative>
    void applyNullMapImpl(const ColumnUInt8& map);
};

ColumnPtr makeNullable(const ColumnPtr& column);

} // namespace doris::vectorized
