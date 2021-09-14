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

#ifndef DORIS_BE_SRC_QUERY_EXEC_TEXT_CONVERTER_HPP
#define DORIS_BE_SRC_QUERY_EXEC_TEXT_CONVERTER_HPP

#include <boost/algorithm/string.hpp>

#include "olap/utils.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "text_converter.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "util/types.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
namespace doris {

// Note: this function has a codegen'd version.  Changing this function requires
// corresponding changes to CodegenWriteSlot.
inline bool TextConverter::write_column(const SlotDescriptor* slot_desc,
                                        vectorized::MutableColumnPtr* column_ptr, const char* data,
                                        int len, bool copy_string, bool need_escape,
                                        MemPool* pool) {
    //小批量导入只有\N被认为是NULL,没有批量导入的replace_value函数
    if (true == slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr->get());
        if (len == 2 && data[0] == '\\' && data[1] == 'N') {
            nullable_column->insert_data(nullptr, 0);
            return true;
        } else {
            nullable_column->get_null_map_data().push_back(0);
            column_ptr = reinterpret_cast<vectorized::MutableColumnPtr*>(
                    &nullable_column->get_nested_column());
        }
    }
    StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;

    // Parse the raw-text data. Translate the text string to internal format.
    switch (slot_desc->type().type) {
    case TYPE_HLL:
    case TYPE_VARCHAR:
    case TYPE_CHAR: {
        StringValue str_val(const_cast<char*>(data),len);
        if (len != 0 && (copy_string || need_escape)) {
            DCHECK(pool != NULL);
            char* val_data = reinterpret_cast<char*>(pool->allocate(len));
            if (need_escape) {
                unescape_string(data, val_data, &str_val.len);
            } else {
                memcpy(val_data, data, len);
            }
            str_val.ptr=val_data;
        }
        reinterpret_cast<vectorized::ColumnString*>(column_ptr)->insert_data(str_val.ptr, str_val.len);
        break;
    }

    case TYPE_BOOLEAN: {
        bool num = StringParser::string_to_bool(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(column_ptr)
                ->insert_value((uint8_t)num);
        break;
    }
    case TYPE_TINYINT: {
        int8_t num = StringParser::string_to_int<int8_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t num = StringParser::string_to_int<int16_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_INT: {
        int32_t num = StringParser::string_to_int<int32_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_BIGINT: {
        int64_t num = StringParser::string_to_int<int64_t>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_LARGEINT: {
        __int128 num = StringParser::string_to_int<__int128>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(column_ptr)
                ->insert_value(num);
        break;
    }

    case TYPE_FLOAT: {
        float num = StringParser::string_to_float<float>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_DOUBLE: {
        double num = StringParser::string_to_float<double>(data, len, &parse_result);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(column_ptr)
                ->insert_value(num);
        break;
    }
    case TYPE_DATE: {
        DateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
            break;
        }
        ts_slot.cast_to_date();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(column_ptr)
                ->insert_data(reinterpret_cast<char*>(&ts_slot), 0);
        break;
    }

    case TYPE_DATETIME: {
        DateTimeValue ts_slot;
        if (!ts_slot.from_date_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
        }
        ts_slot.to_datetime();
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(column_ptr)
                ->insert_data(reinterpret_cast<char*>(&ts_slot), 0);
        break;
    }

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_slot;
        if (decimal_slot.parse_from_str(data, len)) {
            parse_result = StringParser::PARSE_FAILURE;
        }
        PackedInt128 num = binary_cast<DecimalV2Value, PackedInt128>(decimal_slot);
        reinterpret_cast<vectorized::ColumnVector<doris::PackedInt128>*>(column_ptr)
                ->insert_value(num.value);
        break;
    }

    default:
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        break;
    }

    // TODO: add warning for overflow case
    if (parse_result != StringParser::PARSE_SUCCESS) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr->get());
        nullable_column->insert_data(nullptr, 0);
        return false;
    }

    return true;
}

} // namespace doris

#endif
