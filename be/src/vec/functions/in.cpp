#include "vec/columns/columns_number.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_set.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** in(x, set) - function for evaluating the IN
  * notIn(x, set) - and NOT IN.
  */

template <bool negative, bool null_in_set>
struct FunctionInName;

template <>
struct FunctionInName<false, false>
{
    static constexpr auto name = "in";
};


template <>
struct FunctionInName<true, false>
{
    static constexpr auto name = "not_in";
};

template <>
struct FunctionInName<false, true>
{
    static constexpr auto name = "in_null_in_set";
};


template <>
struct FunctionInName<true, true>
{
    static constexpr auto name = "not_in_null_in_set";
};


template <bool negative, bool null_in_set>
class FunctionIn : public IFunction
{
public:
    static constexpr auto name = FunctionInName<negative, null_in_set>::name;
//    static FunctionPtr create(const Context &)
    static FunctionPtr create()
    {
        return std::make_shared<FunctionIn>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return makeNullable(std::make_shared<DataTypeUInt8>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        /// NOTE: after updating this code, check that FunctionIgnoreExceptNull returns the same type of column.

        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
        const ColumnSet * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        auto set = column_set->getData();
        /// First argument may be a single column.
        const ColumnWithTypeAndName & left_arg = block.getByPosition(arguments[0]);

        auto res = ColumnUInt8::create();
        ColumnUInt8::Container & vec_res = res->getData();
        vec_res.resize(left_arg.column->size());

        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create(left_arg.column->size());
        auto& vec_null_map_to = col_null_map_to->getData();

       if (set->size() == 0) {
            if (negative)
                memset(vec_res.data(), 1, vec_res.size());
            else
                memset(vec_res.data(), 0, vec_res.size());
        } else {
           auto materialized_column = left_arg.column->convertToFullColumnIfConst();
           auto size = materialized_column->size();

           if (auto * nullable = checkAndGetColumn<ColumnNullable>(*materialized_column)) {
               const auto& nested_column = nullable->getNestedColumn();
               const auto& null_map = nullable->getNullMapColumn().getData();

               for (size_t i = 0; i < size; ++i) {
                   vec_null_map_to[i] = null_map[i];
                   if (null_map[i]) { continue;}
                   const auto &ref_data = nested_column.getDataAt(i);
                   vec_res[i] = negative ^ set->find((void *) ref_data.data, ref_data.size);
                   if constexpr (null_in_set) {
                       vec_null_map_to[i] = negative == vec_res[i];
                   }
               }
           } else {
               /// For all rows
               for (size_t i = 0; i < size; ++i) {
                   const auto &ref_data = materialized_column->getDataAt(i);
                   vec_res[i] = negative ^ set->find((void *) ref_data.data, ref_data.size);
                   if constexpr (null_in_set) {
                       vec_null_map_to[i] = negative == vec_res[i];
                   } else {
                       vec_null_map_to[i] = 0;
                   }
               }
           }
       }

       block.getByPosition(result).column = ColumnNullable::create(std::move(res), std::move(col_null_map_to));
       return Status::OK();
    }
};

void registerFunctionIn(SimpleFunctionFactory & factory)
{
    factory.registerFunction<FunctionIn<false, false>>();
    factory.registerFunction<FunctionIn<true, false>>();
    factory.registerFunction<FunctionIn<true, true>>();
    factory.registerFunction<FunctionIn<false, true>>();
}

}
