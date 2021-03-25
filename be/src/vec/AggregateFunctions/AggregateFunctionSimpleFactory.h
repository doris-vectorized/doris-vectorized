#pragma once

#include <vec/AggregateFunctions/IAggregateFunction.h>
#include <vec/Core/Field.h>
#include <vec/DataTypes/IDataType.h>

#include <functional>
#include <iostream>
#include <memory>
#include <unordered_map>

namespace DB {
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
using AggregateFunctionCreator =
        std::function<AggregateFunctionPtr(const std::string&, const DataTypes&, const Array&)>;
class AggregateFunctionSimpleFactory {
public:
    using Creator = AggregateFunctionCreator;

private:
    using AggregateFunctions = std::unordered_map<std::string, Creator>;

    AggregateFunctions aggregate_functions;

public:
    void registerFunction(const std::string& name, Creator creator) {
        aggregate_functions[name] = creator;
    }

    AggregateFunctionPtr get(const std::string& name, const DataTypes& argument_types,
                             const Array& parameters) {
        return aggregate_functions[name](name, argument_types, parameters);
    }
};
}; // namespace DB
