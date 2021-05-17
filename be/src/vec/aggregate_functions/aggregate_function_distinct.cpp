#include "vec/aggregate_functions/aggregate_function_distinct.h"

#include <algorithm>

#include "boost/algorithm/string.hpp"
#include "vec/aggregate_functions/aggregate_function_combinator.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"
// #include "registerAggregateFunctions.h"

namespace doris::vectorized {
namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorDistinct final : public IAggregateFunctionCombinator {
public:
    String getName() const override { return "Distinct"; }

    DataTypes transformArguments(const DataTypes& arguments) const override {
        if (arguments.empty())
            throw Exception(
                    "Incorrect number of arguments for aggregate function with Distinct suffix",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(const AggregateFunctionPtr& nested_function,
                                                    const DataTypes& arguments,
                                                    const Array& params) const override {
        AggregateFunctionPtr res;
        if (arguments.size() == 1) {
            res.reset(createWithNumericType<AggregateFunctionDistinct,
                                            AggregateFunctionDistinctSingleNumericData>(
                    *arguments[0], nested_function, arguments));

            if (res) return res;

            if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return std::make_shared<AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<true>>>(nested_function,
                                                                           arguments);
            else
                return std::make_shared<AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<false>>>(nested_function,
                                                                            arguments);
        }

        return std::make_shared<
                AggregateFunctionDistinct<AggregateFunctionDistinctMultipleGenericData>>(
                nested_function, arguments);
    }
};

const std::string DISTINCT_FUNCTION_PREFIX = "multi_distinct_";

void registerAggregateFunctionCombinatorDistinct(AggregateFunctionSimpleFactory& factory) {
    AggregateFunctionCreator creator = [&](const std::string& name, const DataTypes& types,
                                           const Array& params) {
        // 1. we should get not nullable types;
        DataTypes nested_types(types.size());
        std::transform(types.begin(), types.end(), nested_types.begin(),
                       [](const auto& e) { return removeNullable(e); });
        auto function_combinator = std::make_shared<AggregateFunctionCombinatorDistinct>();
        auto transformArguments = function_combinator->transformArguments(nested_types);
        if (!boost::algorithm::starts_with(name, DISTINCT_FUNCTION_PREFIX)) {
            return AggregateFunctionPtr();
        }
        auto nested_function_name = name.substr(DISTINCT_FUNCTION_PREFIX.size());
        auto nested_function = factory.get(nested_function_name, transformArguments, params);
        return function_combinator->transformAggregateFunction(nested_function, types, params);
    };
    factory.registerDistinctFunctionCombinator(creator, DISTINCT_FUNCTION_PREFIX);
    // factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinct>());
}
} // namespace doris::vectorized
