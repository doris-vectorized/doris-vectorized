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

#include "vec/data_types/data_type_string.h"

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/io/io_helper.h"

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace doris::vectorized {

template <typename Reader>
static inline void read(IColumn& column, Reader&& reader) {
    ColumnString& column_string = assert_cast<ColumnString&>(column);
    ColumnString::Chars& data = column_string.get_chars();
    ColumnString::Offsets& offsets = column_string.get_offsets();
    size_t old_chars_size = data.size();
    size_t old_offsets_size = offsets.size();
    try {
        reader(data);
        data.push_back(0);
        offsets.push_back(data.size());
    } catch (...) {
        offsets.resize_assume_reserved(old_offsets_size);
        data.resize_assume_reserved(old_chars_size);
        throw;
    }
}

std::string DataTypeString::to_string(const IColumn& column, size_t row_num) const {
    const StringRef& s =
            assert_cast<const ColumnString&>(*column.convert_to_full_column_if_const().get())
                    .get_data_at(row_num);
    return s.to_string();
}

Field DataTypeString::getDefault() const {
    return String();
}

MutableColumnPtr DataTypeString::createColumn() const {
    return ColumnString::create();
}

bool DataTypeString::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

void DataTypeString::serialize(const IColumn& column, PColumn* pcolumn) const {
    std::ostringstream buf;
    for (size_t i = 0; i < column.size(); ++i) {
        const auto& s = assert_cast<const ColumnString&>(*column.convert_to_full_column_if_const().get())
                                .get_data_at(i);
        write_string_binary(s, buf);
    }
    write_binary(buf, pcolumn);
}

void DataTypeString::deserialize(const PColumn& pcolumn, IColumn* column) const {
    ColumnString* column_string = assert_cast<ColumnString*>(column);
    ColumnString::Chars& data = column_string->get_chars();
    ColumnString::Offsets& offsets = column_string->get_offsets();
    size_t offset = 0;
    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);
    std::istringstream istr(uncompressed);
    while (istr.peek() != EOF) {
        std::string s;
        read_binary(s, istr);
        size_t size = s.size();
        offset = offset + size + 1;
        offsets.push_back(offset);
        data.resize(offset);
        s.copy(reinterpret_cast<char*>(&data[offset - size - 1]), size);
        data.back() = 0;
    }
}
} // namespace doris::vectorized
