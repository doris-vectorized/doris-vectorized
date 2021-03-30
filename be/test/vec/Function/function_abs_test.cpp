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

#include "vec/Functions/abs.hpp"

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "runtime/tuple_row.h"
#include "runtime/row_batch.h"
#include "exec/schema_scanner.h"

namespace doris {

TEST(BlockTest, RowBatchCovertToBlock) {
    SchemaScanner::ColumnDesc column_descs[] = {
                {"k1", TYPE_SMALLINT, sizeof(int16_t), false},
                {"k2", TYPE_INT,      sizeof(int32_t), false},
                {"k3", TYPE_DOUBLE,    sizeof(double),   false}};
    SchemaScanner schema_scanner(column_descs, 3);
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
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, column_descs[0].size);
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, column_descs[1].size);
        slot_desc = tuple_desc->slots()[2];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k3, column_descs[2].size);

        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    DB::FunctionAbs function_abs;
    std::shared_ptr<DB::IFunction> abs_function_ptr = function_abs.create();
    auto block = row_batch.conver_to_vec_block();
    // 1. build arguments
    DB::ColumnNumbers arguments;
    arguments.emplace_back(block.getPositionByName("k1"));

    // 2. build result column
    size_t num_columns_without_result = block.columns();
    block.insert({ nullptr, block.getByPosition(0).type, "abs(k1)"});

    abs_function_ptr->execute(block, arguments, num_columns_without_result, 1024, false);

    k1 = -100;
    for (int i = 0; i < 1024; ++i) {
        DB::ColumnPtr column = block.getColumns()[3];
        ASSERT_EQ(column->getInt(i), std::abs(k1++));
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
