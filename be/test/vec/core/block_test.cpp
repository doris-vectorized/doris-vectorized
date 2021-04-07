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

#include "vec/core/block.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

TEST(BlockTest, RowBatchCovertToBlock) {
    SchemaScanner::ColumnDesc column_descs[] = {
                {"k1", TYPE_SMALLINT, sizeof(int16_t), true},
                {"k2", TYPE_INT,      sizeof(int32_t), false},
                {"k3", TYPE_DOUBLE,    sizeof(double),   false},
                {"k4", TYPE_VARCHAR,    sizeof(StringValue),   false},
                {"k5", TYPE_DECIMALV2,    sizeof(DecimalV2Value),   false},
                {"k6", TYPE_LARGEINT,    sizeof(__int128),   false},
                {"k7", TYPE_DATETIME,    sizeof(__int128),   false}};

    SchemaScanner schema_scanner(column_descs, sizeof(column_descs) / sizeof(SchemaScanner::ColumnDesc));
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);

    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());

    int16_t k1 = -100;
    int32_t k2 = 100000;
    double k3 = 7.7;

    for (int i = 0; i < 1024; ++i, k1++, k2++, k3 += 0.1) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);

        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        if (i % 5 == 0) {
            tuple->set_null(slot_desc->null_indicator_offset());
        } else {
            memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, column_descs[0].size);
        }
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k3, column_descs[2].size);

        // string slot
        slot_desc = tuple_desc->slots()[3];
        auto num_str = std::to_string(k1);
        auto string_slot = tuple->get_string_slot(slot_desc->tuple_offset());
        string_slot->ptr = (char*)row_batch.tuple_data_pool()->allocate(num_str.size());
        string_slot->len = num_str.size();
        memcpy(string_slot->ptr, num_str.c_str(), num_str.size());

        slot_desc = tuple_desc->slots()[4];
        DecimalV2Value decimalv2_num(std::to_string(k3));
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &decimalv2_num, column_descs[4].size);

        slot_desc = tuple_desc->slots()[5];
        int128_t k6 = k1;
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k6 ,column_descs[5].size);

        slot_desc = tuple_desc->slots()[6];
        DateTimeValue k7;
        std::string now_time("2020-12-02");
        k7.from_date_str(now_time.c_str(), now_time.size());
        TimeInterval time_interval(TimeUnit::DAY, k1, false);
        k7.date_add_interval(time_interval, TimeUnit::DAY);
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k7 ,column_descs[6].size);

        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    auto block = row_batch.convert_to_vec_block();
    k1 = -100;
    k2 = 100000;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i) {
        vectorized::ColumnPtr column1 = block.getColumns()[0];
        vectorized::ColumnPtr column2 = block.getColumns()[1];
        vectorized::ColumnPtr column3 = block.getColumns()[2];
        vectorized::ColumnPtr column4 = block.getColumns()[3];
        vectorized::ColumnPtr column5 = block.getColumns()[4];
        vectorized::ColumnPtr column6 = block.getColumns()[5];
        vectorized::ColumnPtr column7 = block.getColumns()[6];

        if (i % 5 != 0) {
            ASSERT_EQ((int16_t)column1->get64(i), k1);
        } else {
            ASSERT_TRUE(column1->isNullAt(i));
        }
        ASSERT_EQ(column2->getInt(i), k2++);
        ASSERT_EQ(column3->getFloat64(i), k3);
        ASSERT_STREQ(column4->getDataAt(i).data, std::to_string(k1).c_str());
        auto decimal_field = column5->operator[](i)
                                     .get<vectorized::DecimalField<vectorized::Decimal128>>();
        DecimalV2Value decimalv2_num(std::to_string(k3));
        ASSERT_EQ(DecimalV2Value(decimal_field.getValue()), decimalv2_num);

        int128_t larget_int = k1;
        ASSERT_EQ(column6->operator[](i).get<vectorized::Int128>(), k1);

        larget_int = column7->operator[](i).get<vectorized::Int128>();
        DateTimeValue k7;
        memcpy(&k7, &larget_int ,column_descs[6].size);
        DateTimeValue date_time_value;
        std::string now_time("2020-12-02");
        date_time_value.from_date_str(now_time.c_str(), now_time.size());
        TimeInterval time_interval(TimeUnit::DAY, k1, false);
        date_time_value.date_add_interval(time_interval, TimeUnit::DAY);

        ASSERT_EQ(k7, date_time_value);

        k1++;
        k3 += 0.1;
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
