#include <iostream>
#include <string>

#include "gtest/gtest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_types_number.h"

namespace doris::vectorized {
// declare function
void registerAggregateFunctionSum(AggregateFunctionSimpleFactory& factory);

TEST(AggTest, basic_test) {
    auto column_vector_int32 = ColumnVector<Int32>::create();
    for (int i = 0; i < 4096; i++) {
        column_vector_int32->insert(castToNearestFieldType(i));
    }
    // test implement interface
    AggregateFunctionSimpleFactory factory;
    registerAggregateFunctionSum(factory);
    DataTypePtr data_type(std::make_shared<DataTypeInt32>());
    DataTypes data_types = {data_type};
    Array array;
    auto agg_function = factory.get("sum", data_types, array);
    AggregateDataPtr place = (char*)malloc(sizeof(uint64_t) * 4096);
    agg_function->create(place);
    const IColumn* column[1] = {column_vector_int32.get()};
    for (int i = 0; i < 4096; i++) {
        agg_function->add(place, column, i, nullptr);
    }
    int ans = 0;
    for (int i = 0; i < 4096; i++) {
        ans += i;
    }
    ASSERT_EQ(ans, *(int32_t*)place);
    agg_function->destroy(place);
}
} // namespace doris::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
