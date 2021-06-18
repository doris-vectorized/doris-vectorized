#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int LOGICAL_ERROR;
}

template <typename T>
struct AggregateFunctionAvgData {
    T sum = 0;
    UInt64 count = 0;

    template <typename ResultT>
    ResultT result() const {
        if constexpr (std::is_floating_point_v<ResultT>)
            if constexpr (std::numeric_limits<ResultT>::is_iec559)
                return static_cast<ResultT>(sum) / count; /// allow division by zero

        if (!count)
            throw Exception("AggregateFunctionAvg with zero values", ErrorCodes::LOGICAL_ERROR);
        return static_cast<ResultT>(sum) / count;
    }

    void write(std::ostream& buf) const {
        writeBinary(sum, buf);
        writeBinary(count, buf);
    }

    void read(std::istream& buf) {
        readBinary(sum, buf);
        readBinary(count, buf);
    }
};

/// Calculates arithmetic mean of numbers.
template <typename T, typename Data>
class AggregateFunctionAvg final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>> {
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<Decimal128>,
                                              DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>,
                                            ColumnVector<Float64>>;

    /// ctor for native types
    AggregateFunctionAvg(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvg(const IDataType& data_type, const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>(argument_types_,
                                                                                {}),
              scale(getDecimalScale(data_type)) {}

    String getName() const override { return "avg"; }

    DataTypePtr getReturnType() const override {
        if constexpr (IsDecimalNumber<T>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).sum += column.getData()[row_num];
        ++this->data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, std::ostream& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, std::istream& buf, Arena*) const override {
        this->data(place).read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.getData().push_back(this->data(place).template result<ResultType>());
    }

    const char* getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 scale;
};

} // namespace doris::vectorized
