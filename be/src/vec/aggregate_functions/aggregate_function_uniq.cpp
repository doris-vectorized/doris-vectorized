#include <vec/aggregate_functions/aggregate_function_simple_factory.h>
#include <vec/aggregate_functions/aggregate_function_uniq.h>
#include <vec/aggregate_functions/factory_helpers.h>
#include <vec/aggregate_functions/helpers.h>
#include <vec/data_types/data_type_string.h>

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

template <template <typename> class Data, typename DataForVariadic>
AggregateFunctionPtr createAggregateFunctionUniq(const std::string& name,
                                                 const DataTypes& argument_types,
                                                 const Array& params) {
    assertNoParameters(name, params);

    if (argument_types.empty())
        throw Exception("Incorrect number of arguments for aggregate function " + name,
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() == 1) {
        const IDataType& argument_type = *argument_types[0];

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionUniq, Data>(
                *argument_types[0], argument_types));

        WhichDataType which(argument_type);
        // TODO: DateType
        if (res)
            return res;
        else if (which.isStringOrFixedString())
            return std::make_shared<AggregateFunctionUniq<String, Data<String>>>(argument_types);
    }

    return nullptr;
}

void registerAggregateFunctionsUniq(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator =
            createAggregateFunctionUniq<AggregateFunctionUniqExactData,
                                        AggregateFunctionUniqExactData<String>>;
    factory.registerFunction("multi_distinct_count", creator);
}

} // namespace doris::vectorized
