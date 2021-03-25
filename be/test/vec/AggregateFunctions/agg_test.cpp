#include <iostream>
#include <string>

#include "vec/AggregateFunctions/AggregateFunctionSimpleFactory.h"
#include "vec/AggregateFunctions/IAggregateFunction.h"
#include "vec/Columns/ColumnVector.h"
#include "vec/DataTypes/DataTypesNumber.h"
#include "vec/DataTypes/IDataType.h"
#include "gtest/gtest.h"

namespace DB {
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
} // namespace DB

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
