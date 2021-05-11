#pragma once

#include <city.h>

#include <type_traits>

#include "vec/common/bit_cast.h"

// #include <IO/WriteHelpers.h>
// #include <IO/ReadHelpers.h>

// #include <DataTypes/DataTypesNumber.h>
#include <vec/common/hash_table/hash_set.h>

#include "vec/common/aggregation_common.h"
#include "vec/data_types/data_types_number.h"
// #include <Common/HyperLogLogWithSmallSetOptimization.h>
// #include <Common/CombinedCardinalityEstimator.h>
#include <vec/common/assert_cast.h>
#include <vec/common/typeid_cast.h>

// #include <AggregateFunctions/UniquesHashSet.h>
#include "vec/aggregate_functions/aggregate_function.h"
// #include <AggregateFunctions/UniqVariadicHash.h>

namespace doris::vectorized {

/// uniqExact

template <typename T>
struct AggregateFunctionUniqExactData {
    using Key = T;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, HashCRC32<Key>, HashTableGrower<4>,
                        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 4)>>;

    Set set;

    static String getName() { return "uniqExact"; }
};

/// For rows, we put the SipHash values (128 bits) into the hash table.
template <>
struct AggregateFunctionUniqExactData<String> {
    using Key = UInt128;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>,
                        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

    Set set;

    static String getName() { return "uniqExact"; }
};

namespace detail {

/** The structure for the delegation work to add one element to the `uniq` aggregate functions.
  * Used for partial specialization to add strings.
  */
template <typename T, typename Data>
struct OneAdder {
    static void ALWAYS_INLINE add(Data& data, const IColumn& column, size_t row_num) {
        if constexpr (std::is_same_v<Data, AggregateFunctionUniqExactData<T>>) {
            if constexpr (!std::is_same_v<T, String>) {
                data.set.insert(assert_cast<const ColumnVector<T>&>(column).getData()[row_num]);
            } else {
                StringRef value = column.getDataAt(row_num);

                UInt128 key;
                SipHash hash;
                hash.update(value.data, value.size);
                hash.get128(key.low, key.high);

                data.set.insert(key);
            }
        }
    }
};

} // namespace detail

/// Calculates the number of different values approximately or exactly.
template <typename T, typename Data>
class AggregateFunctionUniq final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>> {
public:
    AggregateFunctionUniq(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionUniq<T, Data>>(argument_types_,
                                                                                 {}) {}

    String getName() const override { return Data::getName(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        detail::OneAdder<T, Data>::add(this->data(place), *columns[0], row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        this->data(place).set.merge(this->data(rhs).set);
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        this->data(place).set.write(buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena*) const override {
        this->data(place).set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).getData().push_back(this->data(place).set.size());
    }

    const char* getHeaderFilePath() const override { return __FILE__; }
};

} // namespace doris::vectorized
