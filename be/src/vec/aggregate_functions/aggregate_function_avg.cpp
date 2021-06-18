#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_avg.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/aggregate_functions/factory_helpers.h"

namespace doris::vectorized
{

namespace
{

template <typename T>
struct Avg
{
    using FieldType = std::conditional_t<IsDecimalNumber<T>, Decimal128, NearestFieldType<T>>;
    using Function = AggregateFunctionAvg<T, AggregateFunctionAvgData<FieldType>>;
};

template <typename T>
using AggregateFuncAvg = typename Avg<T>::Function;

AggregateFunctionPtr createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFuncAvg>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<AggregateFuncAvg>(*data_type, argument_types));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->get_name() + " of argument for aggregate function " + name,
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

}

//void registerAggregateFunctionAvg(AggregateFunctionFactory & factory)
//{
//    factory.registerFunction("avg", createAggregateFunctionAvg, AggregateFunctionFactory::CaseInsensitive);
//}

void registerAggregateFunctionAvg(AggregateFunctionSimpleFactory& factory) {
    factory.registerFunction("avg", createAggregateFunctionAvg);
}
}
