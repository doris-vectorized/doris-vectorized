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

#include "olap/bloom_filter_predicate.h"

#include "exprs/create_predicate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/predicate_column.h"
#include "vec/utils/util.hpp"

using namespace doris::vectorized;

#define APPLY_FOR_PRIMTYPE(M) \
    M(TYPE_TINYINT)           \
    M(TYPE_SMALLINT)          \
    M(TYPE_INT)               \
    M(TYPE_BIGINT)            \
    M(TYPE_LARGEINT)          \
    M(TYPE_FLOAT)             \
    M(TYPE_DOUBLE)            \
    M(TYPE_CHAR)              \
    M(TYPE_DATE)              \
    M(TYPE_DATETIME)          \
    M(TYPE_VARCHAR)           \
    M(TYPE_STRING)

namespace doris {

// blomm filter column predicate do not support in segment v1
template <PrimitiveType type>
void BloomFilterColumnPredicate<type>::evaluate(VectorizedRowBatch* batch) const {
    uint16_t n = batch->size();
    uint16_t* sel = batch->selected();
    if (!batch->selected_in_use()) {
        for (uint16_t i = 0; i != n; ++i) {
            sel[i] = i;
        }
    }
}

template <PrimitiveType type>
void BloomFilterColumnPredicate<type>::evaluate(ColumnBlock* block, uint16_t* sel,
                                                uint16_t* size) const {
    uint16_t new_size = 0;
    if (block->is_nullable()) {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size +=
                    (!block->cell(idx).is_null() && _specific_filter->find_olap_engine(cell_value));
        }
    } else {
        for (uint16_t i = 0; i < *size; ++i) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(block->cell(idx).cell_ptr());
            new_size += _specific_filter->find_olap_engine(cell_value);
        }
    }
    *size = new_size;
}

template <PrimitiveType type>
void BloomFilterColumnPredicate<type>::evaluate(vectorized::IColumn& column, uint16_t* sel,
                                                uint16_t* size) const {
    uint16_t new_size = 0;
    using T = typename PrimitiveTypeTraits<type>::CppType;

    if (column.is_nullable()) {
        auto* nullable_col = check_and_get_column<ColumnNullable>(column);
        auto& null_map_data = nullable_col->get_null_map_column().get_data();
        auto* pred_col =
                check_and_get_column<PredicateColumnType<T>>(nullable_col->get_nested_column());
        auto& pred_col_data = pred_col->get_data();
        for (uint16_t i = 0; i < *size; i++) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(&(pred_col_data[idx]));
            new_size += (!null_map_data[idx]) && _specific_filter->find_olap_engine(cell_value);
        }
    } else {
        auto* pred_col = check_and_get_column<PredicateColumnType<T>>(column);
        auto& pred_col_data = pred_col->get_data();
        for (uint16_t i = 0; i < *size; i++) {
            uint16_t idx = sel[i];
            sel[new_size] = idx;
            const auto* cell_value = reinterpret_cast<const void*>(&(pred_col_data[idx]));
            new_size += _specific_filter->find_olap_engine(cell_value);
        }
    }
    *size = new_size;
}

ColumnPredicate* BloomFilterColumnPredicateFactory::create_column_predicate(
        uint32_t column_id, const std::shared_ptr<IBloomFilterFuncBase>& bloom_filter,
        FieldType type) {
    std::shared_ptr<IBloomFilterFuncBase> filter;
    switch (type) {
#define M(NAME)                                                           \
    case OLAP_FIELD_##NAME: {                                             \
        filter.reset(create_bloom_filter(bloom_filter->tracker(), NAME)); \
        filter->light_copy(bloom_filter.get());                           \
        return new BloomFilterColumnPredicate<NAME>(column_id, filter);   \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case OLAP_FIELD_TYPE_DECIMAL: {
        filter.reset(create_bloom_filter(bloom_filter->tracker(), TYPE_DECIMALV2));
        filter->light_copy(bloom_filter.get());
        return new BloomFilterColumnPredicate<TYPE_DECIMALV2>(column_id, filter);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        filter.reset(create_bloom_filter(bloom_filter->tracker(), TYPE_BOOLEAN));
        filter->light_copy(bloom_filter.get());
        return new BloomFilterColumnPredicate<TYPE_BOOLEAN>(column_id, filter);
    }
    default:
        return nullptr;
    }
}
} //namespace doris
