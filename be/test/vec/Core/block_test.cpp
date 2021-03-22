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

#include "vec/Core/Block.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "runtime/row_batch.h"
#include "exec/schema_scanner.h"

namespace doris {

TEST(ZipUtilTest, basic) {
    SchemaScanner::ColumnDesc[] column_descs = {
                {"k1", TYPE_SMALLINT, sizeof(int16_t), false},
                {"k2", TYPE_INT,      sizeof(int32_T), false},
                {"k3", TYPE_DOUBLE,    sizeof(double),   false} };

    SchemaScanner schema_scanner(column_descs, 3);
    auto tuple_desc = schema_scanner.tuple_desc();
    RowDescriptor row_desc(tuple_desc, false);
    RowBatch row_batch(row_desc, 1024, MemTracker::CreateTracker(-1, "BlockTest", null, false));

    int16_t k1 = -100;
    int32_t k2 = 100000;
    double k3 = 7.7;

    for (int i = 0; i < 1024; ++i, k1++, k2++, k3 += 0.1) {
        auto idx = row_batch.add_row();
        auto tuple_row = row_batch.get_row(idx);

        auto tuple = static_cast<Tuple*>(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, column_descs[0].size);
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k3, column_descs[2].size);

        tuple_row->set_tuple(0, tuple);
    }

    auto block = row_batch.conver_to_vec_block();
    k1 = -100;
    k2 = 100000;
    k3 = 7.7;
    for (int i = 0; i < 1024; ++i) {
        DB::ColumnPtr column1 = block.getColumns()[0];
        DB::ColumnPtr column2 = block.getColumns()[1];
        DB::ColumnPtr column3 = block.getColumns()[2];

        ASSERT_EQ(column1->getInt(i), k1++);
        ASSERT_EQ(column2->getInt(i), k2++);
        ASSERT_EQ(column3->getFloat64(i), k3++);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
