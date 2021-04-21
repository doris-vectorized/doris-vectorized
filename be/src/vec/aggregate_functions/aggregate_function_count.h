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

#pragma once

// #include <IO/VarInt.h>
// #include <IO/WriteHelpers.h>

#include <vec/aggregate_functions/aggregate_function.h>
#include <vec/columns/column_nullable.h>
#include <vec/common/assert_cast.h>
#include <vec/data_types/data_types_number.h>
#include <vec/io/io_helper.h>

#include <array>

namespace doris::vectorized {

struct AggregateFunctionCountData {
    UInt64 count = 0;
};

namespace ErrorCodes {
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
} // namespace ErrorCodes

/// Simply count number of calls.
class AggregateFunctionCount final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount> {
public:
    AggregateFunctionCount(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena*) const override {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnUInt64&>(to).getData().push_back(data(place).count);
    }

    const char* getHeaderFilePath() const override { return __FILE__; }
};

/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
        : public IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                              AggregateFunctionCountNotNullUnary> {
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr& argument, const Array& params)
            : IAggregateFunctionDataHelper<AggregateFunctionCountData,
                                           AggregateFunctionCountNotNullUnary>({argument}, params) {
        if (!argument->isNullable())
            throw Exception(
                    "Logical error: not Nullable data type passed to "
                    "AggregateFunctionCountNotNullUnary",
                    ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        data(place).count += !assert_cast<const ColumnNullable&>(*columns[0]).isNullAt(row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena*) const override {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnUInt64&>(to).getData().push_back(data(place).count);
    }

    const char* getHeaderFilePath() const override { return __FILE__; }
};

} // namespace doris::vectorized
