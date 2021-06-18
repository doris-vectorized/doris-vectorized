#pragma once

#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <typename Impl, bool use_field = true>
class FunctionConst : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionConst>(); }

public:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes& /*arguments*/) const override {
        return Impl::get_return_type();
    }

    Status executeImpl(Block& block, const ColumnNumbers&, size_t result,
                       size_t input_rows_count) override {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                input_rows_count, Impl::init_value());
        return Status::OK();
    }
};

template <typename Impl>
class FunctionConst<Impl, false> : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionConst>(); }
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes& /*arguments*/) const override {
        return Impl::get_return_type();
    }

    Status executeImpl(Block& block, const ColumnNumbers&, size_t result,
                       size_t input_rows_count) override {
        auto column = Impl::ReturnColVec::create();
        column->getData().emplace_back(Impl::init_value());
        block.getByPosition(result).column = ColumnConst::create(std::move(column), 1);
        return Status::OK();
    }
};

template <typename Impl>
class FunctionMathConstFloat64 : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathConstFloat64>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes& /*arguments*/) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    Status executeImpl(Block& block, const ColumnNumbers&, size_t result,
                       size_t input_rows_count) override {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                input_rows_count == 0 ? 1 : input_rows_count, Impl::value);
        return Status::OK();
    }
};

} // namespace doris::vectorized
