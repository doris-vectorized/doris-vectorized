#include "vec/exprs/vexpr.h"

#include <thrift/protocol/TJSONProtocol.h>

#include <iostream>

#include "exec/schema_scanner.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "testutil/desc_tbl_builder.h"

TEST(TEST_VEXPR, ABSTEST) {
    doris::ChunkAllocator::init_instance(4096);
    doris::ObjectPool object_pool;
    doris::DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << doris::TYPE_INT << doris::TYPE_DOUBLE;
    doris::DescriptorTbl* desc_tbl = builder.build();

    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = doris::MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    doris::RowBatch row_batch(row_desc, 1024, tracker_ptr.get());
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    doris::TExpr exprx = apache::thrift::from_json_string<doris::TExpr>(expr_json);
    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(&object_pool, exprx, &context);

    int32_t k1 = -100;
    for (int i = 0; i < 1024; ++i, k1++) {
        auto idx = row_batch.add_row();
        doris::TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple =
                (doris::Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    runtime_stat.set_desc_tbl(desc_tbl);
    std::shared_ptr<doris::MemTracker> tracker = doris::MemTracker::CreateTracker();
    context->prepare(&runtime_stat, row_desc, tracker);
    context->open(&runtime_stat);

    auto block = row_batch.conver_to_vec_block();
    int ts = -1;
    context->execute(&block, &ts);
}

TEST(TEST_VEXPR, ABSTEST2) {
    using namespace doris;
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 1);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    TExpr exprx = apache::thrift::from_json_string<TExpr>(expr_json);

    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(&object_pool, exprx, &context);

    int32_t k1 = -100;
    for (int i = 0; i < 1024; ++i, k1++) {
        auto idx = row_batch.add_row();
        doris::TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple =
                (doris::Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    DescriptorTbl desc_tbl;
    desc_tbl._slot_desc_map[0] = tuple_desc->slots()[0];
    runtime_stat.set_desc_tbl(&desc_tbl);
    std::shared_ptr<doris::MemTracker> tracker = doris::MemTracker::CreateTracker();
    context->prepare(&runtime_stat, row_desc, tracker);
    context->open(&runtime_stat);

    auto block = row_batch.conver_to_vec_block();
    int ts = -1;
    context->execute(&block, &ts);

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
