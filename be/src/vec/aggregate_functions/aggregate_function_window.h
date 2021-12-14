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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/src/Processors/Transforms/WindowTransform.h
// and modified by Doris

#pragma once

#include <istream>
#include <ostream>
#include <type_traits>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct RowNumberData {
    int64_t count;
};

class WindowFunctionRowNumber final
        : public IAggregateFunctionDataHelper<RowNumberData, WindowFunctionRowNumber> {
public:
    WindowFunctionRowNumber(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "row_number"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void add_range_single_place(int64_t frame_start, int64_t frame_end, AggregateDataPtr place,
                                const IColumn** columns, Arena* arena, int64_t end) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override { this->data(place).count = 0; }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
    const char* get_header_file_path() const override { return __FILE__; }
};

struct RankData {
    int64_t rank;
    int64_t count;
    int64_t peer_group_start;
};

class WindowFunctionRank final : public IAggregateFunctionDataHelper<RankData, WindowFunctionRank> {
public:
    WindowFunctionRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t frame_start, int64_t frame_end, AggregateDataPtr place,
                                const IColumn** columns, Arena* arena, int64_t end) const override {
        int64_t peer_group_count = frame_end - frame_start;
        if (this->data(place).peer_group_start != frame_start) {
            this->data(place).peer_group_start = frame_start;
            this->data(place).rank += this->data(place).count;
        }
        this->data(place).count = peer_group_count;
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).rank = 0;
        this->data(place).count = 1;
        this->data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
    const char* get_header_file_path() const override { return __FILE__; }
};

struct DenseRankData {
    int64_t rank;
    int64_t peer_group_start;
};
class WindowFunctionDenseRank final
        : public IAggregateFunctionDataHelper<DenseRankData, WindowFunctionDenseRank> {
public:
    WindowFunctionDenseRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "dense_rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t frame_start, int64_t frame_end, AggregateDataPtr place,
                                const IColumn** columns, Arena* arena, int64_t end) const override {
        if (this->data(place).peer_group_start != frame_start) {
            this->data(place).peer_group_start = frame_start;
            this->data(place).rank++;
        }
    }

    void reset(AggregateDataPtr place) const override {
        this->data(place).rank = 0;
        this->data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
    const char* get_header_file_path() const override { return __FILE__; }
};

template <typename T, bool is_nullable, bool is_string>
struct LeadAndLagData {
public:
    bool has_init() const { return _is_init; }

    void reset() {
        if (has_init()) {
            _value = {};
            _default_value = {};
            _is_null = false;
            _defualt_is_null = false;
            _is_init = false;
        }
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (is_nullable) {
            if (_is_null) {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                if constexpr (is_string)
                    col.insert_data(_value.data, _value.size);
                else
                    col.insert_data(_value.data, 0);
            }
        } else {
            if constexpr (is_string) {
                auto& col = assert_cast<ColumnString&>(to);
                col.insert_data(_value.data, _value.size);
            } else {
                auto& col = assert_cast<ColumnVector<T>&>(to);
                col.insert_data(_value.data, 0);
            }
        }
    }

    void get_result(size_t frame_start, size_t frame_end, const IColumn** columns, Arena* arena, size_t end) {
        if constexpr (is_nullable) {
            const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0]);
            if (nullable_column && nullable_column->is_null_at(frame_end - 1)) {
                _is_null = true;
                return;
            }
            if constexpr (is_string) {
                const auto* sources = check_and_get_column<ColumnString>(
                        nullable_column->get_nested_column_ptr().get());
                _value = sources->get_data_at(frame_end - 1);
            } else {
                const auto* sources = check_and_get_column<ColumnVector<T>>(
                        nullable_column->get_nested_column_ptr().get());
                _value = sources->get_data_at(frame_end - 1);
            }
        } else {
            _is_null = false;
            if constexpr (is_string) {
                const auto* sources = check_and_get_column<ColumnString>(columns[0]);
                _value = sources->get_data_at(frame_end - 1);
            } else {
                const auto* sources = check_and_get_column<ColumnVector<T>>(columns[0]);
                _value = sources->get_data_at(frame_end - 1);
            }
        }
    }

    bool defualt_is_null() { return _defualt_is_null; }

    void set_is_null() { _is_null = true; }

    void set_value() { _value = _default_value; }

    void check_default(const IColumn* column) {
        if (!has_init()) {
            if (is_column_nullable(*column)) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(column);
                if (nullable_column->is_null_at(0)) {
                    _defualt_is_null = true;
                }
            } else {
                if constexpr (is_string) {
                    const auto& col = static_cast<const ColumnString&>(*column);
                    _default_value = col.get_data_at(0);
                } else {
                    const auto& col = static_cast<const ColumnVector<T>&>(*column);
                    _default_value = col.get_data_at(0);
                }
            }
            _is_init = true;
        }
    }

private:
    StringRef _value;
    StringRef _default_value;
    bool _is_init = false;
    bool _is_null = false;
    bool _defualt_is_null = false;
};

template <typename Data>
struct WindowFunctionLeadData : Data {
    void add_range_single_place(size_t frame_start, size_t frame_end, const IColumn** columns,
                                Arena* arena, size_t end) {
        this->check_default(columns[2]);
        if (frame_end > end) { //output default value
            if (this->defualt_is_null()) {
                this->set_is_null();
            } else {
                this->set_value();
            }
            return;
        }
        this->get_result(frame_start, frame_end, columns, arena, end);
    }
    static const char* name() { return "lead"; }
};

template <typename Data>
struct WindowFunctionLagData : Data {
    void add_range_single_place(int64_t frame_start, int64_t frame_end, const IColumn** columns,
                                Arena* arena, int64_t end) {
        this->check_default(columns[2]);
        if (frame_start >= frame_end) { //[unbound preceding(0), offset preceding(-123)]
            if (this->defualt_is_null()) {
                this->set_is_null();
            } else {
                this->set_value();
            }
            return;
        }
        this->get_result(frame_start, frame_end, columns, arena, end);
    }
    static const char* name() { return "lag"; }
};

template <typename Data>
class WindowFunctionLeadLag final
        : public IAggregateFunctionDataHelper<Data, WindowFunctionLeadLag<Data>> {
public:
    WindowFunctionLeadLag(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, WindowFunctionLeadLag<Data>>(argument_types, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return Data::name(); }
    DataTypePtr get_return_type() const override { return _argument_type; }

    void add_range_single_place(int64_t frame_start, int64_t frame_end, AggregateDataPtr place,
                                const IColumn** columns, Arena* arena, int64_t end) const override {
        this->data(place).add_range_single_place(frame_start, frame_end, columns, arena, end);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num, Arena*) const override {}
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
    const char* get_header_file_path() const override { return __FILE__; }

private:
    DataTypePtr _argument_type;
};

} // namespace doris::vectorized
