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

#include "vec/common/exception.h"
#include "vec/core/types.h"
// #include <IO/ReadBuffer.h>
// #include <IO/WriteBuffer.h>
// #include <IO/VarInt.h>
// #include <IO/ReadHelpers.h>
// #include <IO/WriteHelpers.h>
#include "vec/core/block_info.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int UNKNOWN_BLOCK_INFO_FIELD;
}

/*
/// Write values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
void BlockInfo::write(WriteBuffer & out) const
{
/// Set of pairs `FIELD_NUM`, value in binary form. Then 0.
#define WRITE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    write_var_uint(FIELD_NUM, out); \
    write_binary(NAME, out);

    APPLY_FOR_BLOCK_INFO_FIELDS(WRITE_FIELD)

#undef WRITE_FIELD
    write_var_uint(0, out);
}

/// Read values in binary form.
void BlockInfo::read(ReadBuffer & in)
{
    UInt64 field_num = 0;

    while (true)
    {
        read_var_uint(field_num, in);
        if (field_num == 0)
            break;

        switch (field_num)
        {
        #define READ_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
            case FIELD_NUM: \
                read_binary(NAME, in); \
                break;

            APPLY_FOR_BLOCK_INFO_FIELDS(READ_FIELD)

        #undef READ_FIELD
            default:
                throw Exception("Unknown BlockInfo field number: " + to_string(field_num), ErrorCodes::UNKNOWN_BLOCK_INFO_FIELD);
        }
    }
}*/

void BlockMissingValues::set_bit(size_t column_idx, size_t row_idx) {
    RowsBitMask& mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask[row_idx] = true;
}

const BlockMissingValues::RowsBitMask& BlockMissingValues::get_defaults_bitmask(
        size_t column_idx) const {
    static RowsBitMask none;
    auto it = rows_mask_by_column_id.find(column_idx);
    if (it != rows_mask_by_column_id.end()) return it->second;
    return none;
}

} // namespace doris::vectorized
