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

#include <gtest/gtest.h>
#include <time.h>

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

TEST(function_string_strleft_test, function_string_strleft_test) {
    int len = 5;
    std::string str_test[len] = {"asd", "hel  lo  ", "hello word", "HELLO,!^%", "MYtestSTR"};
    int32_t length[len] = {1, 5, 20, 7, 3};
    std::string str_expected[len] = {"a", "hel  ", "hello word", "HELLO,!", "NULL"};

    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_VARCHAR, sizeof(StringValue), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 2);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "function_string_strleft_test", nullptr, false);
    RowBatch row_batch(row_desc, len, tracker_ptr.get());

    for (int i = 0; i < len; ++i) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        auto string_slot = tuple->get_string_slot(slot_desc->tuple_offset());
        string_slot->ptr = (char*)row_batch.tuple_data_pool()->allocate(str_test[i].size());
        string_slot->len = str_test[i].size();
        memcpy(string_slot->ptr, str_test[i].c_str(), str_test[i].size());

        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &length[i], column_descs[1].size);
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }
    auto block = row_batch.convert_to_vec_block();
    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));
    arguments.emplace_back(block.get_position_by_name("k2"));
    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1")),
            block.get_by_position(block.get_position_by_name("k2"))};
    vectorized::DataTypePtr data_type(
            doris::vectorized::make_nullable(std::make_shared<vectorized::DataTypeString>()));
    auto str_function = doris::vectorized::SimpleFunctionFactory::instance().get_function(
            "strleft", ctn, data_type);
    size_t num_columns_without_result = block.columns();

    auto null_map = vectorized::ColumnUInt8::create(len, 0);
    auto& null_map_data = null_map->get_data();
    null_map_data[len - 1] = 1;
    auto res = block.get_by_position(0).column;
    block.get_by_position(0).column =
            vectorized::ColumnNullable::create(std::move(res), std::move(null_map));

    block.insert({nullptr, block.get_by_position(0).type, "strleft(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[2];
        doris::vectorized::Field field;
        if (column->is_null_at(i))
        {
            ASSERT_EQ("NULL", str_expected[i]);
            continue;
        }
        column->get(i, field);
        std::string ans = field.get<std::string>();
        ASSERT_EQ(ans, str_expected[i]);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
