#pragma once

#include "vec/data_types/data_types_number.h"
#include "vec/columns/columns_number.h"
#include "vec/functions/function.h"


namespace doris::vectorized {

template<typename Impl>
class FunctionConst : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionConst>(); }

private:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override {
        return Impl::get_return_type();
    }

    void executeImpl(Block &block, const ColumnNumbers &, size_t result, size_t input_rows_count) override {
        block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(input_rows_count,
                                                                                                 Impl::init_value());
    }
};

}
