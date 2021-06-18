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

#include "vec/data_types/data_type_bitmap.h"

#include "vec/columns/column_complex.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
void DataTypeBitMap::serialize(const IColumn& column, PColumn* pcolumn) const {
    std::ostringstream buf;
    auto& data_column = assert_cast<const ColumnBitmap&>(*column.convert_to_full_column_if_const());
    // TODO: remove std::string as memory buffer to avoid memory copy
    std::string memory_buffer;
    for (size_t i = 0; i < column.size(); ++i) {
        auto& bitmap = const_cast<BitmapValue&>(data_column.get_element(i));
        int bytesize = bitmap.getSizeInBytes();
        write_int_binary(bytesize, buf);
        memory_buffer.resize(bytesize);
        bitmap.write(const_cast<char*>(memory_buffer.data()));
        write_binary(memory_buffer, buf);
        memory_buffer.clear();
    }
    write_binary(buf, pcolumn);
}

void DataTypeBitMap::deserialize(const PColumn& pcolumn, IColumn* column) const {
    auto& data_column = assert_cast<ColumnBitmap&>(*column);
    auto& data = data_column.get_data();

    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);

    std::istringstream istr(uncompressed);
    std::string memory_buffer;
    while (istr.peek() != EOF) {
        int bytesize = 0;
        read_int_binary(bytesize, istr);
        read_binary(memory_buffer, istr);

        data.emplace_back();
        data.back().deserialize(memory_buffer.data());
        memory_buffer.clear();
    }
}

MutableColumnPtr DataTypeBitMap::createColumn() const {
    return ColumnBitmap::create();
}
} // namespace doris::vectorized
