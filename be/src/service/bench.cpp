#include <benchmark/benchmark.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <iostream>
#include <string>

#include "exec/schema_scanner.h"
#include "exprs/aggregate_functions.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/math_functions.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/exec_env.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "testutil/desc_tbl_builder.h"
#include "udf/udf_internal.h"
#include "vec/core/block.h"
#include "vec/functions/abs.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_sum.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_types_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

static void BM_ABS_SCALAR(benchmark::State& state) {
    using namespace doris;
    ChunkAllocator::init_instance(4096);

    ObjectPool object_pool;
    DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << TYPE_INT << TYPE_INT << TYPE_DOUBLE;
    DescriptorTbl* desc_tbl = builder.build();

    auto tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());

    row_batch.reset();
    int32_t k1 = -100;
    int32_t k2 = 100000;
    double k3 = 7.7;
    for (int i = 0; i < 1024; ++i, k1++, k2++, k3 += 0.1) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    MathFunctions::init();

    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    TExpr exprx = apache::thrift::from_json_string<TExpr>(expr_json);
    ObjectPool pool;
    RuntimeState runtime_stat(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    runtime_stat.set_desc_tbl(desc_tbl);
    std::shared_ptr<MemTracker> tracker = MemTracker::CreateTracker();
    ExprContext* ctx = nullptr;
    Expr::create_expr_tree(&pool, exprx, &ctx);
    ctx->prepare(&runtime_stat, row_desc, tracker);
    ctx->open(&runtime_stat);

    void* res_ary[1024];
    for (auto _ : state) {
        for (int i = 0; i < 1024; ++i) {
            TupleRow* tuple_row = row_batch.get_row(i);
            auto res = ctx->get_value(tuple_row);
            res_ary[i] = res;
        }
    }
    benchmark::DoNotOptimize(res_ary);
}
// Register the function as a benchmark
BENCHMARK(BM_ABS_SCALAR);

// Define another benchmark
static void BM_ABS_VEC(benchmark::State& state) {
    using namespace doris;
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_SMALLINT, sizeof(int16_t), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false},
                                                {"k3", TYPE_DOUBLE, sizeof(double), false}};
    SchemaScanner schema_scanner(column_descs, 3);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());
    //for (auto _ : state) std::string empty_string;
    auto slot_ref = new SlotRef(tuple_desc->slots()[0], tuple_desc->slots()[0]->type());
    auto context = new ExprContext(slot_ref);
    row_batch.reset();
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
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }
    auto block = row_batch.convert_to_vec_block();
    doris::vectorized::FunctionAbs function_abs;
    std::shared_ptr<vectorized::IFunction> abs_function_ptr = function_abs.create();
    doris::vectorized::ColumnNumbers arguments;
    arguments.emplace_back(block.getPositionByName("k2"));
    doris::vectorized::ColumnPtr column1 = block.getColumns()[0];
    size_t num_columns_without_result = block.columns();
    block.insert({nullptr, block.getByPosition(0).type, "abs(k2)"});
    abs_function_ptr->execute(block, arguments, num_columns_without_result, 1024, false);
    for (auto _ : state) {
        abs_function_ptr->execute(block, arguments, num_columns_without_result, 1024, false);
    }
    benchmark::DoNotOptimize(block);
}
BENCHMARK(BM_ABS_VEC);

// Define another benchmark
static void BM_ABS_VECIMPL(benchmark::State& state) {
    using namespace doris;
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_INT, sizeof(int32_t), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false},
                                                {"k3", TYPE_DOUBLE, sizeof(double), false}};
    SchemaScanner schema_scanner(column_descs, 3);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());
    //for (auto _ : state) std::string empty_string;
    row_batch.reset();
    int32_t k1 = -100;
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
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }
    auto block = row_batch.convert_to_vec_block();

    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    TExpr exprx = apache::thrift::from_json_string<TExpr>(expr_json);
    doris::vectorized::VExprContext* context = nullptr;

    doris::vectorized::VExpr::create_expr_tree(&object_pool, exprx, &context);
    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    DescriptorTbl desc_tbl;
    desc_tbl._slot_desc_map[0] = tuple_desc->slots()[0];
    runtime_stat.set_desc_tbl(&desc_tbl);
    std::shared_ptr<doris::MemTracker> tracker = doris::MemTracker::CreateTracker();
    context->prepare(&runtime_stat, row_desc, tracker);
    context->open(&runtime_stat);

    int ts = -1;

    for (auto _ : state) {
        block = row_batch.convert_to_vec_block();
        context->execute(&block, &ts);
        block.erase(ts);
    }
    benchmark::DoNotOptimize(block);
}
BENCHMARK(BM_ABS_VECIMPL);

static void BM_AGG_COUNST_SCALAR(benchmark::State& state) {
    using namespace doris;
    ChunkAllocator::init_instance(4096);

    ObjectPool object_pool;
    DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << TYPE_SMALLINT << TYPE_INT << TYPE_DOUBLE;
    DescriptorTbl* desc_tbl = builder.build();

    auto tuple_desc = const_cast<TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "ScalarAGG", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());

    int16_t k1 = -100;
    int32_t k2 = 100000;
    double k3 = 7.7;
    for (int i = 0; i < 1024; ++i, k1++, k2++, k3 += 0.1) {
        auto idx = row_batch.add_row();
        TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple = (Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        slot_desc = tuple_desc->slots()[1];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k2, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    RuntimeState runtime_stat(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    runtime_stat.set_desc_tbl(desc_tbl);
    MemPool pool(runtime_stat.instance_mem_tracker().get());
    auto context = FunctionContextImpl::create_context(
            &runtime_stat, &pool,
            doris_udf::FunctionContext::TypeDesc {.type = doris_udf::FunctionContext::TYPE_INT},
            doris_udf::FunctionContext::TypeDesc {.type = doris_udf::FunctionContext::TYPE_INT},
            std::vector<doris_udf::FunctionContext::TypeDesc> {
                    doris_udf::FunctionContext::TypeDesc {
                            .type = doris_udf::FunctionContext::TYPE_INT}},
            0, false);

    // AggregateFunctions::init();
    doris_udf::BigIntVal bigintval;
    auto slot_ref = new SlotRef(tuple_desc->slots()[1], tuple_desc->slots()[1]->type());
    auto slot_expr = new ExprContext(slot_ref);
    for (auto _ : state) {
        for (int i = 0; i < 1024; ++i) {
            TupleRow* tuple_row = row_batch.get_row(i);
            IntVal val = slot_expr->get_int_val(tuple_row);
            AggregateFunctions::count_update(context, &val, &bigintval);
        }
    }
    benchmark::DoNotOptimize(bigintval);
}

BENCHMARK(BM_AGG_COUNST_SCALAR);
namespace doris::vectorized {
void registerAggregateFunctionSum(vectorized::AggregateFunctionSimpleFactory& factory);
}
static void BM_AGG_COUNT_VEC(benchmark::State& state) {
    using namespace doris;

    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_SMALLINT, sizeof(int16_t), false},
                                                {"k2", TYPE_INT, sizeof(int32_t), false},
                                                {"k3", TYPE_DOUBLE, sizeof(double), false}};
    SchemaScanner schema_scanner(column_descs, 3);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    auto tracker_ptr = MemTracker::CreateTracker(-1, "BlockTest", nullptr, false);
    RowBatch row_batch(row_desc, 1024, tracker_ptr.get());
    //for (auto _ : state) std::string empty_string;
    // auto slot_ref = new SlotRef(tuple_desc->slots()[0], tuple_desc->slots()[0]->type());
    // auto context = new ExprContext(slot_ref);
    row_batch.reset();
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
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }
    auto block = row_batch.convert_to_vec_block();
    doris::vectorized::Columns columns = block.getColumns();
    doris::vectorized::AggregateFunctionSimpleFactory factory;
    registerAggregateFunctionSum(factory);
    doris::vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
    doris::vectorized::DataTypes data_types = {data_type};
    doris::vectorized::Array array;
    auto agg_function = factory.get("sum", data_types, array);
    doris::vectorized::AggregateDataPtr place = (char*)malloc(sizeof(uint64_t) * 4096);
    agg_function->create(place);
    const doris::vectorized::IColumn* column[1] = {columns[1].get()};

    // using ResultType = NearestFieldType<T>;
    // using AggregateDataType = AggregateFunctionSumData<ResultType>;
    // using Function = AggregateFunctionSum<T, ResultType, AggregateDataType>;

    doris::vectorized::AggregateFunctionSum<int32_t, int64_t,
                                            vectorized::AggregateFunctionSumData<int64_t>>* func =
            nullptr;
    func = (vectorized::AggregateFunctionSum<
            int32_t, int64_t, vectorized::AggregateFunctionSumData<int64_t>>*)agg_function.get();
    for (auto _ : state) {
        agg_function->addBatchSinglePlace(4096,place,column,nullptr);
        for (int i = 0; i < 4096; i++) {
            // agg_function->add(place, column, i, nullptr);
            func->add(place, column, i, nullptr);
        }
    }

    benchmark::DoNotOptimize(block);
}
BENCHMARK(BM_AGG_COUNT_VEC);

BENCHMARK_MAIN();
