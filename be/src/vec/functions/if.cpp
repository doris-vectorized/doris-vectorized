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

#include "vec/functions/simple_function_factory.h"
#include "vec/functions/function_helpers.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/number_traits.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

template <typename A, typename B, typename ResultType>
struct NumIfImpl
{
    using ArrayCond = PaddedPODArray<UInt8>;
    using ArrayA = PaddedPODArray<A>;
    using ArrayB = PaddedPODArray<B>;
    using ColVecResult = ColumnVector<ResultType>;

    static void vector_vector(const ArrayCond & cond, const ArrayA & a, const ArrayB & b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void vector_constant(const ArrayCond & cond, const ArrayA & a, B b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a[i]) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_vector(const ArrayCond & cond, A a, const ArrayB & b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b[i]);
        block.getByPosition(result).column = std::move(col_res);
    }

    static void constant_constant(const ArrayCond & cond, A a, B b, Block & block, size_t result, UInt32)
    {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container & res = col_res->get_data();

        for (size_t i = 0; i < size; ++i)
            res[i] = cond[i] ? static_cast<ResultType>(a) : static_cast<ResultType>(b);
        block.getByPosition(result).column = std::move(col_res);
    }
};

template <typename A, typename B>
struct NumIfImpl<A, B, NumberTraits::Error>
{
private:
    [[noreturn]] static void throw_error()
    {
        throw Exception("Internal logic error: invalid types of arguments 2 and 3 of if", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
public:
    template <typename... Args> static void vector_vector(Args &&...) { throw_error(); }
    template <typename... Args> static void vector_constant(Args &&...) { throw_error(); }
    template <typename... Args> static void constant_vector(Args &&...) { throw_error(); }
    template <typename... Args> static void constant_constant(Args &&...) { throw_error(); }
};

// todo(wb) support llvm codegen
class FunctionIf : public IFunction {
public:
    static constexpr auto name = "if";
            
    static FunctionPtr create() { return std::make_shared<FunctionIf>(); }
    String get_name() const override {
        return name;
    }

    size_t getNumberOfArguments() const override { return 3; }
    bool useDefaultImplementationForNulls() const override { return false; }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override {
        return getLeastSupertype({arguments[1], arguments[2]});
    }

    static ColumnPtr materializeColumnIfConst(const ColumnPtr & column) {
        return column->convert_to_full_column_if_const();
    }

     static ColumnPtr makeNullableColumnIfNot(const ColumnPtr & column) {
        if (is_column_nullable(*column))
            return column;

        return ColumnNullable::create(
            materializeColumnIfConst(column), ColumnUInt8::create(column->size(), 0));
    }

    static ColumnPtr get_nested_column(const ColumnPtr & column) {
        if (auto * nullable = check_and_get_column<ColumnNullable>(*column))
            return nullable->get_nested_column_ptr();

        return column;
    }

    Status executeGeneric(Block& block, const ColumnUInt8* cond_col,
                          const ColumnWithTypeAndName& then_col_type_name,
                          const ColumnWithTypeAndName& else_col_type_name,
                          size_t result,
                          size_t input_row_count) {
        MutableColumnPtr result_column = block.getByPosition(result).type->createColumn();
        result_column->reserve(input_row_count);

        const IColumn& then_col = *then_col_type_name.column;
        const IColumn& else_col = *else_col_type_name.column;
        bool then_is_const = is_column_const(then_col);
        bool else_is_const = is_column_const(else_col);

        const auto & cond_array = cond_col->get_data();

        if (then_is_const && else_is_const) {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(then_col).get_data_column();
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(else_col).get_data_column();
            for (size_t i = 0; i < input_row_count; i++) {
                if (cond_array[i])
                    result_column->insert_from(then_nested_column, 0);
                else
                    result_column->insert_from(else_nested_column, 0);
            }
        } else if (then_is_const) {
            const IColumn & then_nested_column = assert_cast<const ColumnConst &>(then_col).get_data_column();

            for (size_t i = 0; i < input_row_count; i++)
            {
                if (cond_array[i])
                    result_column->insert_from(then_nested_column, 0);
                else
                    result_column->insert_from(else_col, i);
            }
        } else if (else_is_const) {
            const IColumn & else_nested_column = assert_cast<const ColumnConst &>(else_col).get_data_column();

            for (size_t i = 0; i < input_row_count; i++)
            {
                if (cond_array[i])
                    result_column->insert_from(then_col, i);
                else
                    result_column->insert_from(else_nested_column, 0);
            }
        } else {
            for (size_t i = 0; i < input_row_count; i++)
                result_column->insert_from(cond_array[i] ? then_col : else_col, i);
        }
        block.getByPosition(result).column = std::move(result_column);
        return Status::OK();
    }

    void executeBasicType(Block& block, const ColumnUInt8* cond_col,
                            const ColumnWithTypeAndName& then_col,
                            const ColumnWithTypeAndName& else_col,
                            size_t result, Status& status) {

        auto call = [&](const auto & types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using T0 = typename Types::LeftType;
            using T1 = typename Types::RightType;
            using result_type = typename Types::LeftType;

            // for doris, args type and return type must be sanme beacause of type cast has already done before, so here just need one type;
            // but code still need a better impelement
            using ColVecT0 = ColumnVector<T0>;

            if (auto col_then = check_and_get_column<ColVecT0>(then_col.column.get())) {
                if (auto col_else = check_and_get_column<ColVecT0>(else_col.column.get())) {
                     NumIfImpl<T0, T0, result_type>::vector_vector(
                        cond_col->get_data(), col_then->get_data(), col_else->get_data(), block, result, 0);
                } else if (auto col_const_else = checkAndGetColumnConst<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::vector_constant(
                        cond_col->get_data(), col_then->get_data(), col_const_else->template get_value<T0>(), block, result, 0);
                }
            } else if (auto col_const_then = checkAndGetColumnConst<ColVecT0>(then_col.column.get())) {
                if (auto col_else = check_and_get_column<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::constant_vector(cond_col->get_data(),
                        col_const_then->template get_value<T0>(), col_else->get_data(), block, result, 0);
                } else if (auto col_const_else = checkAndGetColumnConst<ColVecT0>(else_col.column.get())) {
                    NumIfImpl<T0, T0, result_type>::constant_constant(cond_col->get_data(),
                        col_const_then->template get_value<T0>(), col_const_else->template get_value<T0>(), block, result, 0);
                }
            } else {
                status = Status::InternalError("unexpected args column type"); 
            }
            return true;
        };

        // todo(wb): a better way to determine type
        callOnBasicTypes<true, true, false, false>(then_col.type->getTypeId(),
            else_col.type->getTypeId(),call
        );
    }    
    
    bool executeForNullThenElse(Block & block,
                const ColumnWithTypeAndName & arg_cond,
                const ColumnWithTypeAndName & arg_then,
                const ColumnWithTypeAndName & arg_else, 
                size_t result, size_t input_rows_count, Status& status) {
        bool then_is_null = arg_then.column->only_null();
        bool else_is_null = arg_else.column->only_null();

        if (!then_is_null && !else_is_null)
                return false;

        if (then_is_null && else_is_null) {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConstWithDefaultValue(input_rows_count);
            return true;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());

        /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
        if (then_is_null) {
            if (cond_col) {
                if (is_column_nullable(*arg_else.column)) {
                    auto arg_else_column = arg_else.column;
                    auto result_column = (*std::move(arg_else_column)).mutate();
                    assert_cast<ColumnNullable &>(*result_column).apply_null_map(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    block.getByPosition(result).column = std::move(result_column);
                } else {
                    block.getByPosition(result).column = ColumnNullable::create(
                        materializeColumnIfConst(arg_else.column), arg_cond.column);
                }
            } else if (cond_const_col) {
                if (cond_const_col->get_value<UInt8>()) {
                    block.getByPosition(result).column = block.getByPosition(
                            result).type->createColumn()->clone_resized(input_rows_count);
                } else {
                    block.getByPosition(result).column = makeNullableColumnIfNot(arg_else.column);
                }
            } else {
                status = Status::InternalError("Illegal column " + arg_cond.column->get_name() + " of first argument of function " + get_name()
                                               + ". Must be ColumnUInt8 or ColumnConstUInt8.");
            }
            return true;
        }

        /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (else_is_null) {
            if (cond_col) {
                size_t size = input_rows_count;
                auto & null_map_data = cond_col->get_data();

                auto negated_null_map = ColumnUInt8::create();
                auto & negated_null_map_data = negated_null_map->get_data();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i) {
                    negated_null_map_data[i] = !null_map_data[i];
                }

                if (is_column_nullable(*arg_then.column)) {
                    auto arg_then_column = arg_then.column;
                    auto result_column = (*std::move(arg_then_column)).mutate();
                    assert_cast<ColumnNullable &>(*result_column).apply_negated_null_map(assert_cast<const ColumnUInt8 &>(*arg_cond.column));
                    block.getByPosition(result).column = std::move(result_column);
                } else {
                    block.getByPosition(result).column = ColumnNullable::create(
                        materializeColumnIfConst(arg_then.column), std::move(negated_null_map));
                }
            } else if (cond_const_col) {
                if (cond_const_col->get_value<UInt8>()) {
                    block.getByPosition(result).column = makeNullableColumnIfNot(arg_then.column);
                } else {
                    block.getByPosition(result).column = block.getByPosition(
                            result).type->createColumn()->clone_resized(input_rows_count);
                }
            } else {
                status = Status::InternalError("Illegal column " + arg_cond.column->get_name() + " of first argument of function " + get_name()
                                               + ". Must be ColumnUInt8 or ColumnConstUInt8.");
            }
            return true;
        }

        return false;
    }
    
    bool executeForNullableThenElse(Block & block, 
        const ColumnWithTypeAndName & arg_cond,
        const ColumnWithTypeAndName & arg_then,
        const ColumnWithTypeAndName & arg_else,
        size_t result, size_t input_rows_count) {

        auto * then_is_nullable = check_and_get_column<ColumnNullable>(*arg_then.column);
        auto * else_is_nullable = check_and_get_column<ColumnNullable>(*arg_else.column);

        if (!then_is_nullable && !else_is_nullable)
            return false;

        /** Calculate null mask of result and nested column separately.
          */
        ColumnPtr result_null_mask;
        {
            Block temporary_block(
            {
                arg_cond,
                {
                    then_is_nullable
                        ? then_is_nullable->get_null_map_column_ptr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    else_is_nullable
                        ? else_is_nullable->get_null_map_column_ptr()
                        : DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count),
                    std::make_shared<DataTypeUInt8>(),
                    ""
                },
                {
                    nullptr,
                    std::make_shared<DataTypeUInt8>(),
                    ""
                }
            });

            executeImpl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());
            
            result_null_mask = temporary_block.getByPosition(3).column;
        }

        ColumnPtr result_nested_column;

        {
            Block temporary_block(
            {
                arg_cond,
                {
                    get_nested_column(arg_then.column),
                    removeNullable(arg_then.type),
                    ""
                },
                {
                    get_nested_column(arg_else.column),
                    removeNullable(arg_else.type),
                    ""
                },
                {
                    nullptr,
                    removeNullable(block.getByPosition(result).type),
                    ""
                }
            });

            executeImpl(temporary_block, {0, 1, 2}, 3, temporary_block.rows());

            result_nested_column = temporary_block.getByPosition(3).column;
        }

        block.getByPosition(result).column = ColumnNullable::create(
            materializeColumnIfConst(result_nested_column), materializeColumnIfConst(result_null_mask));
        return true;
    }
    
    bool executeForNullCondition(Block & block, const ColumnWithTypeAndName & arg_cond, 
            const ColumnWithTypeAndName & arg_then,
            const ColumnWithTypeAndName & arg_else,
            size_t result) {
        bool cond_is_null = arg_cond.column->only_null();

        if (cond_is_null){
            block.getByPosition(result).column = std::move(arg_else.column);
            return true;
        }

        return false;
    }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override {
        const ColumnWithTypeAndName & arg_cond = block.getByPosition(arguments[0]);
        const ColumnWithTypeAndName & arg_then = block.getByPosition(arguments[1]);
        const ColumnWithTypeAndName & arg_else = block.getByPosition(arguments[2]);

        /// A case for identical then and else (pointers are the same).
        if (arg_then.column.get() == arg_else.column.get()) {
            /// Just point result to them.
            block.getByPosition(result).column = arg_then.column;
            return Status::OK();
        }

        Status ret = Status::OK();
        if (executeForNullCondition(block, arg_cond, arg_then, arg_else, result)
            || executeForNullThenElse(block, arg_cond, arg_then, arg_else, result, input_rows_count, ret)
            || executeForNullableThenElse(block, arg_cond, arg_then, arg_else, result, input_rows_count)) {
            return ret;
        }

        const ColumnUInt8 * cond_col = typeid_cast<const ColumnUInt8 *>(arg_cond.column.get());
        const ColumnConst * cond_const_col = checkAndGetColumnConst<ColumnVector<UInt8>>(arg_cond.column.get());

        if (cond_const_col) {
            block.getByPosition(result).column = cond_const_col->get_value<UInt8>()
                ? arg_then.column
                : arg_else.column;
            return Status::OK();
        }

        if (!cond_col) {
            return Status::InvalidArgument("Illegal column " + arg_cond.column->get_name() + " of first argument of function " + get_name()
                                           + ",Must be ColumnUInt8 or ColumnConstUInt8.");
        }

        WhichDataType which_type(arg_then.type);
        if (which_type.isInt() || which_type.isFloat()) {
            Status status;
            executeBasicType(block, cond_col, arg_then, arg_else, result, status);
            return status;
        } else {
            return executeGeneric(block, cond_col, arg_then, arg_else, result, input_rows_count);
        }

        return Status::OK();
    }

};

void registerFunctionIf(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionIf>();
}

} // namespace;