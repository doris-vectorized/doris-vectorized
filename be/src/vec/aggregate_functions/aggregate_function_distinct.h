#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/key_holder_helpers.h"

// #include <Columns/ColumnArray.h>
// #include <DataTypes/DataTypeArray.h>
#include "vec/common/aggregation_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/sip_hash.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <typename T>
struct AggregateFunctionDistinctSingleNumericData {
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;
    Set set;

    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena*) {
        const auto& vec = assert_cast<const ColumnVector<T>&>(*columns[0]).get_data();
        set.insert(vec[row_num]);
    }

    void merge(const Self& rhs, Arena*) { set.merge(rhs.set); }

    void serialize(std::ostream& buf) const { set.write(buf); }

    void deserialize(std::istream& buf, Arena*) { set.read(buf); }

    MutableColumns getArguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());
        for (const auto& elem : set) argument_columns[0]->insert(elem.get_value());

        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData {
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;
    Set set;

    void merge(const Self& rhs, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        for (const auto& elem : rhs.set)
            set.emplace(ArenaKeyHolder{elem.get_value(), *arena}, it, inserted);
    }

    void serialize(std::ostream& buf) const {
        write_var_uint(set.size(), buf);
        for (const auto& elem : set) write_string_binary(elem.get_value(), buf);
    }

    void deserialize(std::istream& buf, Arena* arena) {
        size_t size;
        read_var_uint(size, buf);
        for (size_t i = 0; i < size; ++i) set.insert(read_string_binary_into(*arena, buf));
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData {
    void add(const IColumn** columns, size_t /* columns_num */, size_t row_num, Arena* arena) {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    MutableColumns getArguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());
        for (const auto& elem : set)
            deserializeAndInsert<is_plain_column>(elem.get_value(), *argument_columns[0]);

        return argument_columns;
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData {
    void add(const IColumn** columns, size_t columns_num, size_t row_num, Arena* arena) {
        const char* begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i) {
            auto cur_ref = columns[i]->serialize_value_into_arena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        Set::LookupResult it;
        bool inserted;
        auto key_holder = SerializedKeyHolder{value, *arena};
        set.emplace(key_holder, it, inserted);
    }

    MutableColumns getArguments(const DataTypes& argument_types) const {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto& elem : set) {
            const char* begin = elem.get_value().data;
            for (auto& column : argument_columns)
                begin = column->deserialize_and_insert_from_arena(begin);
        }

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename Data>
class AggregateFunctionDistinct
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>> {
private:
    static constexpr auto prefix_size = sizeof(Data);
    AggregateFunctionPtr nested_func;
    size_t arguments_num;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(
                      arguments, nested_func_->getParameters()),
              nested_func(nested_func_),
              arguments_num(arguments.size()) {}

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena* arena) const override {
        this->data(place).deserialize(buf, arena);
    }

    // void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override
    void insertResultInto(ConstAggregateDataPtr targetplace, IColumn& to) const override {
        auto place = const_cast<AggregateDataPtr>(targetplace);
        auto arguments = this->data(place).getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) arguments_raw[i] = arguments[i].get();

        assert(!arguments.empty());
        // nested_func->addBatchSinglePlace(arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        // nested_func->insertResultInto(getNestedPlace(place), to, arena);

        nested_func->addBatchSinglePlace(arguments[0]->size(), getNestedPlace(place),
                                         arguments_raw.data(), nullptr);
        nested_func->insertResultInto(getNestedPlace(place), to);
    }

    size_t sizeOfData() const override { return prefix_size + nested_func->sizeOfData(); }

    void create(AggregateDataPtr place) const override {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        this->data(place).~Data();
        nested_func->destroy(getNestedPlace(place));
    }

    String get_name() const override { return nested_func->get_name() + "Distinct"; }

    DataTypePtr getReturnType() const override { return nested_func->getReturnType(); }

    bool allocatesMemoryInArena() const override { return true; }

    const char* getHeaderFilePath() const override { return __FILE__; }

    // AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

} // namespace doris::vectorized
