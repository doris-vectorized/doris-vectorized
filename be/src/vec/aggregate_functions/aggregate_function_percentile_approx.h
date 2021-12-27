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

#include "common/status.h"
#include "util/counts.h"
#include "util/tdigest.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"
namespace doris::vectorized {

struct PercentileApproxState {
    static constexpr double INIT_QUANTILE = -1.0;
    PercentileApproxState() : digest(new TDigest()) {}
    PercentileApproxState(double compression) : digest(new TDigest(compression)) {}
    ~PercentileApproxState() { delete digest; }

    void write(BufferWritable& buf) const {
        uint32_t serialize_size = digest->serialized_size();
        std::string result(serialize_size + sizeof(double), '0');
        memcpy(result.data(), &targetQuantile, sizeof(double));
        digest->serialize((uint8_t*)result.c_str() + sizeof(double));
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        StringRef ref;
        read_binary(ref, buf);
        memcpy(&targetQuantile, ref.data, sizeof(double));
        digest->unserialize((uint8_t*)ref.data + sizeof(double));
    }

    double get() const { return digest->quantile(targetQuantile); }

    void reset() {
        targetQuantile = INIT_QUANTILE;
        delete digest;
        digest = nullptr;
    }

    TDigest* digest = nullptr;
    double targetQuantile = INIT_QUANTILE;
};

class AggregateFunctionPercentileApprox final
        : public IAggregateFunctionDataHelper<PercentileApproxState,
                                              AggregateFunctionPercentileApprox> {
public:
    AggregateFunctionPercentileApprox(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileApproxState,
                                           AggregateFunctionPercentileApprox>(argument_types_, {}),
              _column_size(argument_types_.size()) {}

    String get_name() const override { return "percentile_approx"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Float64>&>(*columns[0]);
        this->data(place).digest->add(sources.get_float64(row_num));
        const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);
        this->data(place).targetQuantile = quantile.get_float64(row_num);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).digest->merge(this->data(rhs).digest);
        if (this->data(place).targetQuantile == PercentileApproxState::INIT_QUANTILE) {
            this->data(place).targetQuantile = this->data(rhs).targetQuantile;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnVector<Float64>&>(to);
        col.insert_value(this->data(place).get());
    }

    const char* get_header_file_path() const override { return __FILE__; }

private:
    size_t _column_size;
};

struct PercentileState {
    Counts counts;
    double quantile = -1.0;

    void write(BufferWritable& buf) const {
        uint32_t serialize_size = counts.serialized_size();
        std::string result(serialize_size + sizeof(double), '0');
        memcpy(result.data(), &quantile, sizeof(double));
        counts.serialize((uint8_t*)result.c_str() + sizeof(double));
        write_binary(result, buf);
    }

    void read(BufferReadable& buf) {
        StringRef ref;
        read_binary(ref, buf);
        memcpy(&quantile, ref.data, sizeof(double));
        counts.unserialize((uint8_t*)ref.data + sizeof(double));
    }

    double get() const {
        auto result = counts.terminate(quantile); //DoubleVal
        return result.val;
    }
};

class AggregateFunctionPercentile final
        : public IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentile> {
public:
    AggregateFunctionPercentile(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<PercentileState, AggregateFunctionPercentile>(
                      argument_types_, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return "percentile"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeFloat64>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& sources = static_cast<const ColumnVector<Int64>&>(*columns[0]);
        this->data(place).counts.increment(sources.get_int(row_num), 1);

        const auto& quantile = static_cast<const ColumnVector<Float64>&>(*columns[1]);
        this->data(place).quantile = quantile.get_float64(row_num);
    }

    void reset(AggregateDataPtr __restrict place) const override {
        this->data(place).counts = {};
        this->data(place).quantile = -1;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).counts.merge(&(this->data(rhs).counts));
        if (this->data(place).quantile == -1.0) {
            this->data(place).quantile = this->data(rhs).quantile;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& col = assert_cast<ColumnVector<Float64>&>(to);
        col.insert_value(this->data(place).get());
    }

    const char* get_header_file_path() const override { return __FILE__; }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
