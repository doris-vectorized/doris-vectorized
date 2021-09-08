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
#include "vec/functions/function_string.h"

#include <gtest/gtest.h>
#include <time.h>

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "function_test_util.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {

TEST(function_string_substr_test, function_string_substr_test) {
    int len = 6;
    std::string str_test[len] = {"asd你好",   "hello word", "hello word",
                                 "HELLO,!^%", "",           "MYtestSTR"};
    int32_t pos[len] = {4, -5, 1, 4, 5, 5};
    int32_t length[len] = {10, 5, 12, 2, 4, 4};
    std::string str_expected[len] = {
            "\xE4\xBD\xA0\xE5\xA5\xBD", " word", "hello word", "LO", "NULL", "NULL"};
    //你好
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_VARCHAR, sizeof(StringValue), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false},
                                                {"k3", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 3);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "function_string_substr_test", nullptr, false);
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
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &pos[i], column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &length[i], column_descs[2].size);
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }
    auto block = row_batch.convert_to_vec_block();
    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));
    arguments.emplace_back(block.get_position_by_name("k2"));
    arguments.emplace_back(block.get_position_by_name("k3"));
    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1")),
            block.get_by_position(block.get_position_by_name("k2")),
            block.get_by_position(block.get_position_by_name("k3"))};
    vectorized::DataTypePtr data_type(
            doris::vectorized::make_nullable(std::make_shared<vectorized::DataTypeString>()));
    auto str_function = doris::vectorized::SimpleFunctionFactory::instance().get_function(
            "substr", ctn, data_type);
    size_t num_columns_without_result = block.columns();

    auto null_map = vectorized::ColumnUInt8::create(len, 0);
    auto& null_map_data = null_map->get_data();
    null_map_data[len - 1] = 1;
    auto res = block.get_by_position(0).column;
    block.get_by_position(0).column =
            vectorized::ColumnNullable::create(std::move(res), std::move(null_map));

    block.insert({nullptr, block.get_by_position(0).type, "substr(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[3];
        doris::vectorized::Field field;
        if (column->is_null_at(i)) {
            ASSERT_EQ("NULL", str_expected[i]);
            continue;
        }
        column->get(i, field);
        std::string ans = field.get<std::string>();
        ASSERT_EQ(ans, str_expected[i]);
    }
}

TEST(function_string_strright_test, function_string_strright_test) {
    int len = 6;
    std::string str_test[len] = {"asd", "hello word", "hello word", "HELLO,!^%", "", "MYtestSTR"};
    int32_t length[len] = {1, -2, 20, 2, 3, 3};
    std::string str_expected[len] = {"d", "ello word", "hello word", "^%", "", "NULL"};

    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_VARCHAR, sizeof(StringValue), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 2);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr =
            MemTracker::CreateTracker(-1, "function_string_strright_test", nullptr, false);
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
            "strright", ctn, data_type);
    size_t num_columns_without_result = block.columns();

    auto null_map = vectorized::ColumnUInt8::create(len, 0);
    auto& null_map_data = null_map->get_data();
    null_map_data[len - 1] = 1;
    auto res = block.get_by_position(0).column;
    block.get_by_position(0).column =
            vectorized::ColumnNullable::create(std::move(res), std::move(null_map));

    block.insert({nullptr, block.get_by_position(0).type, "strright(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[2];
        doris::vectorized::Field field;
        if (column->is_null_at(i)) {
            ASSERT_EQ("NULL", str_expected[i]);
            continue;
        }
        column->get(i, field);
        std::string ans = field.get<std::string>();
        ASSERT_EQ(ans, str_expected[i]);
    }
}

TEST(function_string_strleft_test, function_string_strleft_test) {
    int len = 6;
    std::string str_test[len] = {"asd", "hel  lo  ", "hello word", "HELLO,!^%", "", "MYtestSTR"};
    int32_t length[len] = {1, 5, 20, 7, 2, 3};
    std::string str_expected[len] = {"a", "hel  ", "hello word", "HELLO,!", "NULL", "NULL"};

    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_VARCHAR, sizeof(StringValue), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 2);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr =
            MemTracker::CreateTracker(-1, "function_string_strleft_test", nullptr, false);
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
        if (column->is_null_at(i)) {
            ASSERT_EQ("NULL", str_expected[i]);
            continue;
        }
        column->get(i, field);
        std::string ans = field.get<std::string>();
        ASSERT_EQ(ans, str_expected[i]);
    }
}

TEST(function_string_lower_test, function_string_lower_test) {
    int len = 5;
    std::string str_test[len] = {"", "ASD", "HELLO123", "HELLO,!^%", "MYtestSTR"};
    std::string str_expected[len] = {"", "asd", "hello123", "hello,!^%", "myteststr"};

    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < len; ++i) {
        strcol->insert_data(str_test[i].c_str(), str_test[i].size());
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, "k1");
    vectorized::Block block({type_and_name});

    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));

    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1"))};

    auto str_function = doris::vectorized::SimpleFunctionFactory::instance().get_function(
            "lower", ctn, data_type);
    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, block.get_by_position(0).type, "lower(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[1];
        ASSERT_STREQ(column->get_data_at(i).data, str_expected[i].c_str());
    }
}

TEST(function_string_upper_test, function_string_upper_test) {
    int len = 5;
    std::string str_test[len] = {"", "asd", "hello123", "HELLO,!^%", "MYtestStr"};
    std::string str_expected[len] = {"", "ASD", "HELLO123", "HELLO,!^%", "MYTESTSTR"};

    auto strcol = vectorized::ColumnString::create();
    for (int i = 0; i < len; ++i) {
        strcol->insert_data(str_test[i].c_str(), str_test[i].size());
    }
    vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
    vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type, "k1");
    vectorized::Block block({type_and_name});

    vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.get_position_by_name("k1"));

    doris::vectorized::ColumnsWithTypeAndName ctn = {
            block.get_by_position(block.get_position_by_name("k1"))};

    doris::vectorized::SimpleFunctionFactory factory;
    doris::vectorized::register_function_string(factory);
    auto str_function =
            factory.get_function("upper", ctn, std::make_shared<vectorized::DataTypeString>());

    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, block.get_by_position(0).type, "upper(k1)"});

    str_function->execute(block, arguments, num_columns_without_result, len, false);

    for (int i = 0; i < len; ++i) {
        vectorized::ColumnPtr column = block.get_columns()[1];
        ASSERT_STREQ(column->get_data_at(i).data, str_expected[i].c_str());
    }
}

TEST(function_string_test, function_append_trailing_char_if_absent_test) {
    std::string func_name = "append_trailing_char_if_absent";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::vector<std::any>>> data_set = {
            {{std::string("ASD"), std::string("D")}, {std::string("ASD"), false}},
            {{std::string("AS"), std::string("D")}, {std::string("ASD"), false}},
            {{std::string(""), std::string("")}, {std::string(""), true}},
            {{std::string(""), std::string("A")}, {std::string("A"), false}}};

    vectorized::TypeIndex return_type = vectorized::TypeIndex::String;

    bool is_nullable = true;

    vectorized::check_function(func_name, input_types, data_set, return_type, is_nullable);
}

TEST(function_string_test, function_lpad_test) {
    std::string func_name = "lpad";

    std::vector<vectorized::TypeIndex> input_types = {vectorized::TypeIndex::String,
                                                      vectorized::TypeIndex::Int32,
                                                      vectorized::TypeIndex::String};

    std::vector<std::pair<std::vector<std::any>, std::vector<std::any>>> data_set = {
            {{std::string("hi"), 5, std::string("?")}, {std::string("???hi"), false}},
            {{std::string("g8%7IgY%AHx7luNtf8Kh"), 20, std::string("")},
             {std::string("g8%7IgY%AHx7luNtf8Kh"), false}},
            {{std::string("hi"), 1, std::string("?")}, {std::string("h"), false}},
            {{std::string("你好"), 1, std::string("?")}, {std::string("你"), false}},
            {{std::string("hi"), 0, std::string("?")}, {std::string(""), false}},
            {{std::string("hi"), -1, std::string("?")}, {std::string(""), true}},
            {{std::string("h"), 1, std::string("")}, {std::string("h"), false}},
            {{std::string("hi"), 5, std::string("")}, {std::string(""), true}},
            {{std::string("hi"), 5, std::string("ab")}, {std::string("abahi"), false}},
            {{std::string("hi"), 5, std::string("呵呵")}, {std::string("呵呵呵hi"), false}},
            {{std::string("呵呵"), 5, std::string("hi")}, {std::string("hih呵呵"), false}}};

    vectorized::TypeIndex return_type = vectorized::TypeIndex::String;

    bool is_nullable = true;

    vectorized::check_function(func_name, input_types, data_set, return_type, is_nullable);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
