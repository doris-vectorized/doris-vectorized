// #include <AggregateFunctions/AggregateFunctionFactory.h>
#include <vec/aggregate_functions/aggregate_function_count.h>
#include <vec/aggregate_functions/factory_helpers.h>


namespace DB
{

namespace
{

// AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters)
// {
//     assertNoParameters(name, parameters);
//     assertArityAtMost<1>(name, argument_types);

//     return std::make_shared<AggregateFunctionCount>(argument_types);
// }

}

// void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
// {
//     factory.registerFunction("count", createAggregateFunctionCount, AggregateFunctionFactory::CaseInsensitive);
// }

}
