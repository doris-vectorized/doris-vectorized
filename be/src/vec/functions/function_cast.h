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

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_common.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/string_buffer.hpp"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_types_decimal.h"
#include "vec/data_types/data_types_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {
namespace ErrorCodes {
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int CANNOT_PARSE_NUMBER;
extern const int CANNOT_READ_ARRAY_FROM_TEXT;
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
extern const int CANNOT_PARSE_QUOTED_STRING;
extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
extern const int CANNOT_PARSE_DATE;
extern const int CANNOT_PARSE_DATETIME;
extern const int CANNOT_PARSE_TEXT;
extern const int CANNOT_PARSE_UUID;
extern const int TOO_LARGE_STRING_SIZE;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int LOGICAL_ERROR;
extern const int TYPE_MISMATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
} // namespace ErrorCodes

/** Type conversion functions.
  * toType - conversion in "natural way";
  */

inline UInt32 extractToDecimalScale(const ColumnWithTypeAndName& named_column) {
    const auto* arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type) ||
              checkAndGetDataType<DataTypeUInt32>(arg_type) ||
              checkAndGetDataType<DataTypeUInt16>(arg_type) ||
              checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception("Illegal type of toDecimal() scale " + named_column.type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    Field field;
    named_column.column->get(0, field);
    return field.get<UInt32>();
}

/** Conversion of number types to each other, enums to numbers, dates and datetimes to numbers and back: done by straight assignment.
  *  (Date is represented internally as number of days from some day; DateTime - as unix timestamp)
  */
template <typename FromDataType, typename ToDataType, typename Name>
struct ConvertImpl {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    template <typename Additions = void*>
    static void NO_SANITIZE_UNDEFINED execute(Block& block, const ColumnNumbers& arguments,
                                              size_t result, size_t /*input_rows_count*/,
                                              Additions additions [[maybe_unused]] = Additions()) {
        const ColumnWithTypeAndName& named_from = block.getByPosition(arguments[0]);

        using ColVecFrom =
                std::conditional_t<IsDecimalNumber<FromFieldType>, ColumnDecimal<FromFieldType>,
                                   ColumnVector<FromFieldType>>;
        using ColVecTo = std::conditional_t<IsDecimalNumber<ToFieldType>,
                                            ColumnDecimal<ToFieldType>, ColumnVector<ToFieldType>>;

        if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
            if constexpr (!IsDataTypeDecimalOrNumber<FromDataType> ||
                          !IsDataTypeDecimalOrNumber<ToDataType>)
                throw Exception("Illegal column " + named_from.column->getName() +
                                        " of first argument of function " + Name::name,
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        if (const ColVecFrom* col_from = checkAndGetColumn<ColVecFrom>(named_from.column.get())) {
            typename ColVecTo::MutablePtr col_to = nullptr;
            if constexpr (IsDataTypeDecimal<ToDataType>) {
                UInt32 scale = additions;
                col_to = ColVecTo::create(0, scale);
            } else
                col_to = ColVecTo::create();

            const auto& vec_from = col_from->getData();
            auto& vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i) {
                if constexpr (IsDataTypeDecimal<FromDataType> || IsDataTypeDecimal<ToDataType>) {
                    if constexpr (IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convertDecimals<FromDataType, ToDataType>(
                                vec_from[i], vec_from.getScale(), vec_to.getScale());
                    else if constexpr (IsDataTypeDecimal<FromDataType> &&
                                       IsDataTypeNumber<ToDataType>)
                        vec_to[i] = convertFromDecimal<FromDataType, ToDataType>(
                                vec_from[i], vec_from.getScale());
                    else if constexpr (IsDataTypeNumber<FromDataType> &&
                                       IsDataTypeDecimal<ToDataType>)
                        vec_to[i] = convertToDecimal<FromDataType, ToDataType>(vec_from[i],
                                                                               vec_to.getScale());
                } else
                    vec_to[i] = static_cast<ToFieldType>(vec_from[i]);
            }

            block.getByPosition(result).column = std::move(col_to);
        } else
            throw Exception("Illegal column " + named_from.column->getName() +
                                    " of first argument of function " + Name::name,
                            ErrorCodes::ILLEGAL_COLUMN);
    }
};

/** If types are identical, just take reference to column.
  */
template <typename T, typename Name>
struct ConvertImpl<std::enable_if_t<!T::is_parametric, T>, T, Name> {
    static void execute(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) {
        block.getByPosition(result).column = block.getByPosition(arguments[0]).column;
    }
};

// Date/DateTime

// Generic conversion of any type to String.
struct ConvertImplGenericToString {
    static void execute(Block& block, const ColumnNumbers& arguments, size_t result) {
        const auto& col_with_type_and_name = block.getByPosition(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        size_t size = col_from.size();

        auto col_to = ColumnString::create();

        ColumnString::Chars& data_to = col_to->getChars();
        ColumnString::Offsets& offsets_to = col_to->getOffsets();

        offsets_to.resize(size);

        VectorBufferWriter<ColumnString::Chars> write_buffer(data_to);

        for (size_t i = 0; i < size; ++i) {
            type.to_string(col_from, i, write_buffer);
            offsets_to[i] = write_buffer.count();
        }

        block.getByPosition(result).column = std::move(col_to);
    }
};

template <typename ToDataType, typename Name>
struct ConvertImpl<DataTypeString, ToDataType, Name> {
    template <typename Additions = void*>

    static void execute(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/,
                        Additions additions [[maybe_unused]] = Additions()) {
        throw Exception("not support convert from string", ErrorCodes::NOT_IMPLEMENTED);
    }
};

struct NameToString {
    static constexpr auto name = "toString";
};
struct NameToDecimal32 {
    static constexpr auto name = "toDecimal32";
};
struct NameToDecimal64 {
    static constexpr auto name = "toDecimal64";
};
struct NameToDecimal128 {
    static constexpr auto name = "toDecimal128";
};
struct NameToUInt8 {
    static constexpr auto name = "toUInt8";
};
struct NameToUInt16 {
    static constexpr auto name = "toUInt16";
};
struct NameToUInt32 {
    static constexpr auto name = "toUInt32";
};
struct NameToUInt64 {
    static constexpr auto name = "toUInt64";
};
struct NameToInt8 {
    static constexpr auto name = "toInt8";
};
struct NameToInt16 {
    static constexpr auto name = "toInt16";
};
struct NameToInt32 {
    static constexpr auto name = "toInt32";
};
struct NameToInt64 {
    static constexpr auto name = "toInt64";
};
struct NameToFloat32 {
    static constexpr auto name = "toFloat32";
};
struct NameToFloat64 {
    static constexpr auto name = "toFloat64";
};

/// Monotonicity.

struct PositiveMonotonicity {
    static bool has() { return true; }
    static IFunction::Monotonicity get(const IDataType&, const Field&, const Field&) {
        return {true};
    }
};

struct UnknownMonotonicity {
    static bool has() { return false; }
    static IFunction::Monotonicity get(const IDataType&, const Field&, const Field&) {
        return {false};
    }
};

template <typename T>
struct ToNumberMonotonicity {
    static bool has() { return true; }

    static UInt64 divideByRangeOfType(UInt64 x) {
        if constexpr (sizeof(T) < sizeof(UInt64))
            return x >> (sizeof(T) * 8);
        else
            return 0;
    }

    static IFunction::Monotonicity get(const IDataType& type, const Field& left,
                                       const Field& right) {
        if (!type.isValueRepresentedByNumber()) return {};

        /// If type is same, the conversion is always monotonic.
        /// (Enum has separate case, because it is different data type)
        if (checkAndGetDataType<DataTypeNumber<T>>(
                    &type) /*|| checkAndGetDataType<DataTypeEnum<T>>(&type)*/)
            return {true, true, true};

        /// Float cases.

        /// When converting to Float, the conversion is always monotonic.
        if (std::is_floating_point_v<T>) return {true, true, true};

        /// If converting from Float, for monotonicity, arguments must fit in range of result type.
        if (WhichDataType(type).isFloat()) {
            if (left.isNull() || right.isNull()) return {};

            Float64 left_float = left.get<Float64>();
            Float64 right_float = right.get<Float64>();

            if (left_float >= std::numeric_limits<T>::min() &&
                left_float <= std::numeric_limits<T>::max() &&
                right_float >= std::numeric_limits<T>::min() &&
                right_float <= std::numeric_limits<T>::max())
                return {true};

            return {};
        }

        /// Integer cases.

        const bool from_is_unsigned = type.isValueRepresentedByUnsignedInteger();
        const bool to_is_unsigned = std::is_unsigned_v<T>;

        const size_t size_of_from = type.getSizeOfValueInMemory();
        const size_t size_of_to = sizeof(T);

        const bool left_in_first_half = left.isNull() ? from_is_unsigned : (left.get<Int64>() >= 0);

        const bool right_in_first_half =
                right.isNull() ? !from_is_unsigned : (right.get<Int64>() >= 0);

        /// Size of type is the same.
        if (size_of_from == size_of_to) {
            if (from_is_unsigned == to_is_unsigned) return {true, true, true};

            if (left_in_first_half == right_in_first_half) return {true};

            return {};
        }

        /// Size of type is expanded.
        if (size_of_from < size_of_to) {
            if (from_is_unsigned == to_is_unsigned) return {true, true, true};

            if (!to_is_unsigned) return {true, true, true};

            /// signed -> unsigned. If arguments from the same half, then function is monotonic.
            if (left_in_first_half == right_in_first_half) return {true};

            return {};
        }

        /// Size of type is shrinked.
        if (size_of_from > size_of_to) {
            /// Function cannot be monotonic on unbounded ranges.
            if (left.isNull() || right.isNull()) return {};

            if (from_is_unsigned == to_is_unsigned) {
                /// all bits other than that fits, must be same.
                if (divideByRangeOfType(left.get<UInt64>()) ==
                    divideByRangeOfType(right.get<UInt64>()))
                    return {true};

                return {};
            } else {
                /// When signedness is changed, it's also required for arguments to be from the same half.
                /// And they must be in the same half after converting to the result type.
                if (left_in_first_half == right_in_first_half &&
                    (T(left.get<Int64>()) >= 0) == (T(right.get<Int64>()) >= 0) &&
                    divideByRangeOfType(left.get<UInt64>()) ==
                            divideByRangeOfType(right.get<UInt64>()))
                    return {true};

                return {};
            }
        }

        __builtin_unreachable();
    }
};

/** The monotonicity for the `toString` function is mainly determined for test purposes.
  * It is doubtful that anyone is looking to optimize queries with conditions `std::to_string(CounterID) = 34`.
  */
struct ToStringMonotonicity {
    static bool has() { return true; }

    static IFunction::Monotonicity get(const IDataType& type, const Field& left,
                                       const Field& right) {
        IFunction::Monotonicity positive(true, true);
        IFunction::Monotonicity not_monotonic;

        /// `toString` function is monotonous if the argument is Date or DateTime, or non-negative numbers with the same number of symbols.

        // if (checkAndGetDataType<DataTypeDate>(&type)
        //     || typeid_cast<const DataTypeDateTime *>(&type))
        //     return positive;

        if (left.isNull() || right.isNull()) return {};

        if (left.getType() == Field::Types::UInt64 && right.getType() == Field::Types::UInt64) {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0) ||
                                   (floor(log10(left.get<UInt64>())) ==
                                    floor(log10(right.get<UInt64>())))
                           ? positive
                           : not_monotonic;
        }

        if (left.getType() == Field::Types::Int64 && right.getType() == Field::Types::Int64) {
            return (left.get<Int64>() == 0 && right.get<Int64>() == 0) ||
                                   (left.get<Int64>() > 0 && right.get<Int64>() > 0 &&
                                    floor(log10(left.get<Int64>())) ==
                                            floor(log10(right.get<Int64>())))
                           ? positive
                           : not_monotonic;
        }

        return not_monotonic;
    }
};

// Not Support Datetime
// Not Support Convert From String
template <typename ToDataType, typename Name, typename MonotonicityImpl>
class FunctionConvert : public IFunction {
public:
    using Monotonic = MonotonicityImpl;

    static constexpr auto name = Name::name;
    static constexpr bool to_decimal = std::is_same_v<Name, NameToDecimal32> ||
                                       std::is_same_v<Name, NameToDecimal64> ||
                                       std::is_same_v<Name, NameToDecimal128>;

    static FunctionPtr create() { return std::make_shared<FunctionConvert>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isInjective(const Block&) override { return std::is_same_v<Name, NameToString>; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override {
        if (to_decimal && arguments.size() != 2) {
            throw Exception("Number of arguments for function " + getName() +
                                    " doesn't match: passed " + std::to_string(arguments.size()) +
                                    ", should be 2.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        } else if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() +
                                    " doesn't match: passed " + std::to_string(arguments.size()) +
                                    ", should be 1 or 2. Second argument (time zone) is optional "
                                    "only make sense for DateTime.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        else if constexpr (to_decimal) {
            if (!arguments[1].column)
                throw Exception("Second argument for function " + getName() + " must be constant",
                                ErrorCodes::ILLEGAL_COLUMN);

            UInt64 scale = extractToDecimalScale(arguments[1]);

            if constexpr (std::is_same_v<Name, NameToDecimal32>)
                return createDecimal(9, scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal64>)
                return createDecimal(18, scale);
            else if constexpr (std::is_same_v<Name, NameToDecimal128>)
                return createDecimal(38, scale);

            throw Exception("Someting wrong with toDecimalNN()", ErrorCodes::LOGICAL_ERROR);
        } else {
            // TODO: handle datetime
            return std::make_shared<ToDataType>();
        }
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                     size_t input_rows_count) override {
        try {
            executeInternal(block, arguments, result, input_rows_count);
        } catch (Exception& e) {
            /// More convenient error message.
            if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF) {
                e.addMessage("Cannot parse " + block.getByPosition(result).type->getName() +
                             " from " + block.getByPosition(arguments[0]).type->getName() +
                             ", because value is too short");
            } else if (e.code() == ErrorCodes::CANNOT_PARSE_NUMBER ||
                       e.code() == ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT ||
                       e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED ||
                       e.code() == ErrorCodes::CANNOT_PARSE_QUOTED_STRING ||
                       e.code() == ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE ||
                       e.code() == ErrorCodes::CANNOT_PARSE_DATE ||
                       e.code() == ErrorCodes::CANNOT_PARSE_DATETIME ||
                       e.code() == ErrorCodes::CANNOT_PARSE_UUID) {
                e.addMessage("Cannot parse " + block.getByPosition(result).type->getName() +
                             " from " + block.getByPosition(arguments[0]).type->getName());
            }

            throw;
        }
    }

    bool hasInformationAboutMonotonicity() const override { return Monotonic::has(); }

    Monotonicity getMonotonicityForRange(const IDataType& type, const Field& left,
                                         const Field& right) const override {
        return Monotonic::get(type, left, right);
    }

private:
    void executeInternal(Block& block, const ColumnNumbers& arguments, size_t result,
                         size_t input_rows_count) {
        if (!arguments.size())
            throw Exception {"Function " + getName() + " expects at least 1 arguments",
                             ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

        const IDataType* from_type = block.getByPosition(arguments[0]).type.get();

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimal<RightDataType>) {
                if (arguments.size() != 2)
                    throw Exception {"Function " + getName() + " expects 2 arguments for Decimal.",
                                     ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

                const ColumnWithTypeAndName& scale_column = block.getByPosition(arguments[1]);
                UInt32 scale = extractToDecimalScale(scale_column);

                ConvertImpl<LeftDataType, RightDataType, Name>::execute(block, arguments, result,
                                                                        input_rows_count, scale);
            } else
                ConvertImpl<LeftDataType, RightDataType, Name>::execute(block, arguments, result,
                                                                        input_rows_count);
            return true;
        };

        bool done = callOnIndexAndDataType<ToDataType>(from_type->getTypeId(), call);
        if (!done) {
            /// Generic conversion of any type to String.
            if (std::is_same_v<ToDataType, DataTypeString>) {
                ConvertImplGenericToString::execute(block, arguments, result);
            } else
                throw Exception("Illegal type " +
                                        block.getByPosition(arguments[0]).type->getName() +
                                        " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
};

using FunctionToUInt8 = FunctionConvert<DataTypeUInt8, NameToUInt8, ToNumberMonotonicity<UInt8>>;
using FunctionToUInt16 =
        FunctionConvert<DataTypeUInt16, NameToUInt16, ToNumberMonotonicity<UInt16>>;
using FunctionToUInt32 =
        FunctionConvert<DataTypeUInt32, NameToUInt32, ToNumberMonotonicity<UInt32>>;
using FunctionToUInt64 =
        FunctionConvert<DataTypeUInt64, NameToUInt64, ToNumberMonotonicity<UInt64>>;
using FunctionToInt8 = FunctionConvert<DataTypeInt8, NameToInt8, ToNumberMonotonicity<Int8>>;
using FunctionToInt16 = FunctionConvert<DataTypeInt16, NameToInt16, ToNumberMonotonicity<Int16>>;
using FunctionToInt32 = FunctionConvert<DataTypeInt32, NameToInt32, ToNumberMonotonicity<Int32>>;
using FunctionToInt64 = FunctionConvert<DataTypeInt64, NameToInt64, ToNumberMonotonicity<Int64>>;
using FunctionToFloat32 =
        FunctionConvert<DataTypeFloat32, NameToFloat32, ToNumberMonotonicity<Float32>>;
using FunctionToFloat64 =
        FunctionConvert<DataTypeFloat64, NameToFloat64, ToNumberMonotonicity<Float64>>;
// using FunctionToString = FunctionConvert<DataTypeString, NameToString, ToStringMonotonicity>;
using FunctionToDecimal32 =
        FunctionConvert<DataTypeDecimal<Decimal32>, NameToDecimal32, UnknownMonotonicity>;
using FunctionToDecimal64 =
        FunctionConvert<DataTypeDecimal<Decimal64>, NameToDecimal64, UnknownMonotonicity>;
using FunctionToDecimal128 =
        FunctionConvert<DataTypeDecimal<Decimal128>, NameToDecimal128, UnknownMonotonicity>;

template <typename DataType>
struct FunctionTo;
template <>
struct FunctionTo<DataTypeUInt8> {
    using Type = FunctionToUInt8;
};
template <>
struct FunctionTo<DataTypeUInt16> {
    using Type = FunctionToUInt16;
};
template <>
struct FunctionTo<DataTypeUInt32> {
    using Type = FunctionToUInt32;
};
template <>
struct FunctionTo<DataTypeUInt64> {
    using Type = FunctionToUInt64;
};
template <>
struct FunctionTo<DataTypeInt8> {
    using Type = FunctionToInt8;
};
template <>
struct FunctionTo<DataTypeInt16> {
    using Type = FunctionToInt16;
};
template <>
struct FunctionTo<DataTypeInt32> {
    using Type = FunctionToInt32;
};
template <>
struct FunctionTo<DataTypeInt64> {
    using Type = FunctionToInt64;
};
template <>
struct FunctionTo<DataTypeFloat32> {
    using Type = FunctionToFloat32;
};
template <>
struct FunctionTo<DataTypeFloat64> {
    using Type = FunctionToFloat64;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal32>> {
    using Type = FunctionToDecimal32;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal64>> {
    using Type = FunctionToDecimal64;
};
template <>
struct FunctionTo<DataTypeDecimal<Decimal128>> {
    using Type = FunctionToDecimal128;
};

class PreparedFunctionCast : public PreparedFunctionImpl {
public:
    using WrapperType = std::function<void(Block&, const ColumnNumbers&, size_t, size_t)>;

    explicit PreparedFunctionCast(WrapperType&& wrapper_function_, const char* name_)
            : wrapper_function(std::move(wrapper_function_)), name(name_) {}

    String getName() const override { return name; }

protected:
    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                     size_t input_rows_count) override {
        /// drop second argument, pass others
        ColumnNumbers new_arguments {arguments.front()};
        if (arguments.size() > 2)
            new_arguments.insert(std::end(new_arguments), std::next(std::begin(arguments), 2),
                                 std::end(arguments));

        wrapper_function(block, new_arguments, result, input_rows_count);
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    WrapperType wrapper_function;
    const char* name;
};

struct NameCast {
    static constexpr auto name = "CAST";
};

// Now We don't support convert from string
template <typename ToDataType, typename Name>
class FunctionConvertFromString : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionConvertFromString>(); }
    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override {
        throw Exception("not support convert from string", ErrorCodes::NOT_IMPLEMENTED);
    }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                     size_t input_rows_count) override {
        throw Exception("not support convert from string", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class FunctionCast final : public IFunctionBase {
public:
    using WrapperType = std::function<void(Block&, const ColumnNumbers&, size_t, size_t)>;
    using MonotonicityForRange =
            std::function<Monotonicity(const IDataType&, const Field&, const Field&)>;

    FunctionCast(const char* name_, MonotonicityForRange&& monotonicity_for_range_,
                 const DataTypes& argument_types_, const DataTypePtr& return_type_)
            : name(name_),
              monotonicity_for_range(monotonicity_for_range_),
              argument_types(argument_types_),
              return_type(return_type_) {}

    const DataTypes& getArgumentTypes() const override { return argument_types; }
    const DataTypePtr& getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block& /*sample_block*/, const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const override {
        return std::make_shared<PreparedFunctionCast>(
                prepareUnpackDictionaries(getArgumentTypes()[0], getReturnType()), name);
    }

    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    bool hasInformationAboutMonotonicity() const override {
        return static_cast<bool>(monotonicity_for_range);
    }

    Monotonicity getMonotonicityForRange(const IDataType& type, const Field& left,
                                         const Field& right) const override {
        return monotonicity_for_range(type, left, right);
    }

private:
    const char* name;
    MonotonicityForRange monotonicity_for_range;

    DataTypes argument_types;
    DataTypePtr return_type;

    template <typename DataType>
    WrapperType createWrapper(const DataTypePtr& from_type, const DataType* const,
                              bool requested_result_is_nullable) const {
        FunctionPtr function;

        if (requested_result_is_nullable && checkAndGetDataType<DataTypeString>(from_type.get())) {
            /// In case when converting to Nullable type, we apply different parsing rule,
            /// that will not throw an exception but return NULL in case of malformed input.
            function = FunctionConvertFromString<DataType, NameCast>::create();
        } else
            function = FunctionTo<DataType>::Type::create();

        /// Check conversion using underlying function
        { function->getReturnType(ColumnsWithTypeAndName(1, {nullptr, from_type, ""})); }

        return [function](Block& block, const ColumnNumbers& arguments, const size_t result,
                          size_t input_rows_count) {
            function->execute(block, arguments, result, input_rows_count);
        };
    }

    // WrapperType createStringWrapper(const DataTypePtr & from_type) const
    // {
    //     FunctionPtr function = FunctionToString::create();

    //     /// Check conversion using underlying function
    //     {
    //         function->getReturnType(ColumnsWithTypeAndName(1, { nullptr, from_type, "" }));
    //     }

    //     return [function] (Block & block, const ColumnNumbers & arguments, const size_t result, size_t input_rows_count)
    //     {
    //         function->execute(block, arguments, result, input_rows_count);
    //     };
    // }

    template <typename FieldType>
    WrapperType createDecimalWrapper(const DataTypePtr& from_type,
                                     const DataTypeDecimal<FieldType>* to_type) const {
        using ToDataType = DataTypeDecimal<FieldType>;

        TypeIndex type_index = from_type->getTypeId();
        UInt32 precision = to_type->getPrecision();
        UInt32 scale = to_type->getScale();

        WhichDataType which(type_index);
        bool ok = which.isNativeInt() || which.isNativeUInt() || which.isDecimal() ||
                  which.isFloat() || which.isDateOrDateTime() || which.isStringOrFixedString();
        if (!ok)
            throw Exception {"Conversion from " + from_type->getName() + " to " +
                                     to_type->getName() + " is not supported",
                             ErrorCodes::CANNOT_CONVERT_TYPE};

        return [type_index, precision, scale](Block& block, const ColumnNumbers& arguments,
                                              const size_t result, size_t input_rows_count) {
            auto res =
                    callOnIndexAndDataType<ToDataType>(type_index, [&](const auto& types) -> bool {
                        using Types = std::decay_t<decltype(types)>;
                        using LeftDataType = typename Types::LeftType;
                        using RightDataType = typename Types::RightType;

                        ConvertImpl<LeftDataType, RightDataType, NameCast>::execute(
                                block, arguments, result, input_rows_count, scale);
                        return true;
                    });

            /// Additionally check if callOnIndexAndDataType wasn't called at all.
            if (!res) {
                auto to = DataTypeDecimal<FieldType>(precision, scale);
                throw Exception {"Conversion from " + std::string(getTypeName(type_index)) +
                                         " to " + to.getName() + " is not supported",
                                 ErrorCodes::CANNOT_CONVERT_TYPE};
            }
        };
    }

    WrapperType createIdentityWrapper(const DataTypePtr&) const {
        return [](Block& block, const ColumnNumbers& arguments, const size_t result,
                  size_t /*input_rows_count*/) {
            block.getByPosition(result).column = block.getByPosition(arguments.front()).column;
        };
    }

    WrapperType createNothingWrapper(const IDataType* to_type) const {
        ColumnPtr res = to_type->createColumnConstWithDefaultValue(1);
        return [res](Block& block, const ColumnNumbers&, const size_t result,
                     size_t input_rows_count) {
            /// Column of Nothing type is trivially convertible to any other column
            block.getByPosition(result).column =
                    res->cloneResized(input_rows_count)->convertToFullColumnIfConst();
        };
    }

    WrapperType prepareUnpackDictionaries(const DataTypePtr& from_type,
                                          const DataTypePtr& to_type) const {
        const auto& from_nested = from_type;
        const auto& to_nested = to_type;

        if (from_type->onlyNull()) {
            if (!to_nested->isNullable())
                throw Exception {"Cannot convert NULL to a non-nullable type",
                                 ErrorCodes::CANNOT_CONVERT_TYPE};

            return [](Block& block, const ColumnNumbers&, const size_t result,
                      size_t input_rows_count) {
                auto& res = block.getByPosition(result);
                res.column = res.type->createColumnConstWithDefaultValue(input_rows_count)
                                     ->convertToFullColumnIfConst();
            };
        }

        bool skip_not_null_check = false;

        auto wrapper = prepareRemoveNullable(from_nested, to_nested, skip_not_null_check);

        return wrapper;
    }

    WrapperType prepareRemoveNullable(const DataTypePtr& from_type, const DataTypePtr& to_type,
                                      bool skip_not_null_check) const {
        /// Determine whether pre-processing and/or post-processing must take place during conversion.

        bool source_is_nullable = from_type->isNullable();
        bool result_is_nullable = to_type->isNullable();

        auto wrapper =
                prepareImpl(removeNullable(from_type), removeNullable(to_type), result_is_nullable);

        if (result_is_nullable) {
            return [wrapper, source_is_nullable](Block& block, const ColumnNumbers& arguments,
                                                 const size_t result, size_t input_rows_count) {
                /// Create a temporary block on which to perform the operation.
                auto& res = block.getByPosition(result);
                const auto& ret_type = res.type;
                const auto& nullable_type = static_cast<const DataTypeNullable&>(*ret_type);
                const auto& nested_type = nullable_type.getNestedType();

                Block tmp_block;
                if (source_is_nullable)
                    tmp_block = createBlockWithNestedColumns(block, arguments);
                else
                    tmp_block = block;

                size_t tmp_res_index = block.columns();
                tmp_block.insert({nullptr, nested_type, ""});

                /// Perform the requested conversion.
                wrapper(tmp_block, arguments, tmp_res_index, input_rows_count);

                const auto& tmp_res = tmp_block.getByPosition(tmp_res_index);

                /// May happen in fuzzy tests. For debug purpose.
                if (!tmp_res.column)
                    throw Exception("Couldn't convert " +
                                            block.getByPosition(arguments[0]).type->getName() +
                                            " to " + nested_type->getName() + " in " +
                                            " prepareRemoveNullable wrapper.",
                                    ErrorCodes::LOGICAL_ERROR);

                res.column = wrapInNullable(tmp_res.column,
                                            Block({block.getByPosition(arguments[0]), tmp_res}),
                                            {0}, 1, input_rows_count);
            };
        } else if (source_is_nullable) {
            /// Conversion from Nullable to non-Nullable.

            return [wrapper, skip_not_null_check](Block& block, const ColumnNumbers& arguments,
                                                  const size_t result, size_t input_rows_count) {
                Block tmp_block = createBlockWithNestedColumns(block, arguments, result);

                /// Check that all values are not-NULL.
                /// Check can be skipped in case if LowCardinality dictionary is transformed.
                /// In that case, correctness will be checked beforehand.
                if (!skip_not_null_check) {
                    const auto& col = block.getByPosition(arguments[0]).column;
                    const auto& nullable_col = assert_cast<const ColumnNullable&>(*col);
                    const auto& null_map = nullable_col.getNullMapData();

                    if (!memoryIsZero(null_map.data(), null_map.size()))
                        throw Exception {"Cannot convert NULL value to non-Nullable type",
                                         ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN};
                }

                wrapper(tmp_block, arguments, result, input_rows_count);
                block.getByPosition(result).column = tmp_block.getByPosition(result).column;
            };
        } else
            return wrapper;
    }

    /// 'from_type' and 'to_type' are nested types in case of Nullable.
    /// 'requested_result_is_nullable' is true if CAST to Nullable type is requested.
    WrapperType prepareImpl(const DataTypePtr& from_type, const DataTypePtr& to_type,
                            bool requested_result_is_nullable) const {
        if (from_type->equals(*to_type))
            return createIdentityWrapper(from_type);
        else if (WhichDataType(from_type).isNothing())
            return createNothingWrapper(to_type.get());

        WrapperType ret;

        auto make_default_wrapper = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using ToDataType = typename Types::LeftType;

            if constexpr (std::is_same_v<ToDataType, DataTypeUInt8> ||
                          std::is_same_v<ToDataType, DataTypeUInt16> ||
                          std::is_same_v<ToDataType, DataTypeUInt32> ||
                          std::is_same_v<ToDataType, DataTypeUInt64> ||
                          std::is_same_v<ToDataType, DataTypeInt8> ||
                          std::is_same_v<ToDataType, DataTypeInt16> ||
                          std::is_same_v<ToDataType, DataTypeInt32> ||
                          std::is_same_v<ToDataType, DataTypeInt64> ||
                          std::is_same_v<ToDataType, DataTypeFloat32> ||
                          std::is_same_v<ToDataType, DataTypeFloat64>)
            // std::is_same_v<ToDataType, DataTypeDate> ||
            // std::is_same_v<ToDataType, DataTypeDateTime>)
            {
                ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()),
                                    requested_result_is_nullable);
                return true;
            }
            // if constexpr (
            //     std::is_same_v<ToDataType, DataTypeEnum8> ||
            //     std::is_same_v<ToDataType, DataTypeEnum16>)
            // {
            //     ret = createEnumWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()));
            //     return true;
            // }
            if constexpr (std::is_same_v<ToDataType, DataTypeDecimal<Decimal32>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal64>> ||
                          std::is_same_v<ToDataType, DataTypeDecimal<Decimal128>>) {
                ret = createDecimalWrapper(from_type,
                                           checkAndGetDataType<ToDataType>(to_type.get()));
                return true;
            }

            return false;
        };

        if (callOnIndexAndDataType<void>(to_type->getTypeId(), make_default_wrapper)) return ret;

        switch (to_type->getTypeId()) {
        // case TypeIndex::String:
        //     return createStringWrapper(from_type);
        // case TypeIndex::FixedString:
        //     return createFixedStringWrapper(from_type, checkAndGetDataType<DataTypeFixedString>(to_type.get())->getN());

        // case TypeIndex::Array:
        //     return createArrayWrapper(from_type, checkAndGetDataType<DataTypeArray>(to_type.get()));
        // case TypeIndex::Tuple:
        //     return createTupleWrapper(from_type, checkAndGetDataType<DataTypeTuple>(to_type.get()));

        // case TypeIndex::AggregateFunction:
        //     return createAggregateFunctionWrapper(from_type, checkAndGetDataType<DataTypeAggregateFunction>(to_type.get()));
        default:
            break;
        }

        throw Exception {"Conversion from " + from_type->getName() + " to " + to_type->getName() +
                                 " is not supported",
                         ErrorCodes::CANNOT_CONVERT_TYPE};
    }
};

class FunctionBuilderCast : public FunctionBuilderImpl {
public:
    using MonotonicityForRange = FunctionCast::MonotonicityForRange;

    static constexpr auto name = "CAST";
    static FunctionBuilderPtr create() { return std::make_shared<FunctionBuilderCast>(); }

    FunctionBuilderCast() {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

protected:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName& arguments,
                              const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;

        auto monotonicity = getMonotonicityInformation(arguments.front().type, return_type.get());
        return std::make_shared<FunctionCast>(name, std::move(monotonicity), data_types,
                                              return_type);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override {
        const auto type_col = checkAndGetColumnConst<ColumnString>(arguments.back().column.get());
        if (!type_col)
            throw Exception("Second argument to " + getName() +
                                    " must be a constant string describing type",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypeFactory::instance().get(type_col->getValue<String>());
    }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    template <typename DataType>
    static auto monotonicityForType(const DataType* const) {
        return FunctionTo<DataType>::Type::Monotonic::get;
    }

    MonotonicityForRange getMonotonicityInformation(const DataTypePtr& from_type,
                                                    const IDataType* to_type) const {
        if (const auto type = checkAndGetDataType<DataTypeUInt8>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeUInt16>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeUInt32>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeUInt64>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeInt8>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeInt16>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeInt32>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeInt64>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeFloat32>(to_type))
            return monotonicityForType(type);
        if (const auto type = checkAndGetDataType<DataTypeFloat64>(to_type))
            return monotonicityForType(type);
        // if (const auto type = checkAndGetDataType<DataTypeDate>(to_type))
        //     return monotonicityForType(type);
        // if (const auto type = checkAndGetDataType<DataTypeDateTime>(to_type))
        //     return monotonicityForType(type);
        // if (const auto type = checkAndGetDataType<DataTypeString>(to_type))
        //     return monotonicityForType(type);
        // if (isEnum(from_type))
        // {
        //     if (const auto type = checkAndGetDataType<DataTypeEnum8>(to_type))
        //         return monotonicityForType(type);
        //     if (const auto type = checkAndGetDataType<DataTypeEnum16>(to_type))
        //         return monotonicityForType(type);
        // }
        /// other types like Null, FixedString, Array and Tuple have no monotonicity defined
        return {};
    }
};

} // namespace doris::vectorized