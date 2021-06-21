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

#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/functions/function.h"
//#include "vec/functions/extract_time_zone_from_function_arguments.h"
#include "vec/functions/date_time_transforms.h"


namespace doris::vectorized
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// See DateTimeTransforms.h
template <typename ToDataType, typename Transform>
class FunctionDateOrDateTimeToSomething : public IFunction
{
public:
    static constexpr auto name = Transform::name;
    static FunctionPtr create() {
        return std::make_shared<FunctionDateOrDateTimeToSomething>();
    }

    String get_name() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() == 1)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->get_name() + " of argument of function " + get_name()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else if (arguments.size() == 2)
        {
            if (!isDateOrDateTime(arguments[0].type))
                throw Exception(
                    "Illegal type " + arguments[0].type->get_name() + " of argument of function " + get_name()
                        + ". Should be a date or a date with time",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (!isString(arguments[1].type))
                throw Exception(
                    "Function " + get_name() + " supports 1 or 2 arguments. The 1st argument "
                          "must be of type Date or DateTime. The 2nd argument (optional) must be "
                          "a constant string with timezone name",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            if (isDate(arguments[0].type) && std::is_same_v<ToDataType, DataTypeDate>)
                throw Exception(
                    "The timezone argument of function " + get_name() + " is allowed only when the 1st argument has the type DateTime",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        else
            throw Exception(
                "Number of arguments for function " + get_name() + " doesn't match: passed " + std::to_string(arguments.size())
                    + ", should be 1 or 2",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        /// For DateTime, if time zone is specified, attach it to type.
//        if (std::is_same_v<ToDataType, DataTypeDateTime>)
//            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 1, 0));
//        else
            return std::make_shared<ToDataType>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    Status executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const IDataType * from_type = block.get_by_position(arguments[0]).type.get();
        WhichDataType which(from_type);

//        if (which.isDate())
//            DateTimeTransformImpl<DataTypeDate::FieldType, typename ToDataType::FieldType, Transform>::execute(block, arguments, result, input_rows_count);
//        else if (which.isDateTime())
        return DateTimeTransformImpl<Int128, typename ToDataType::FieldType, Transform>::execute(block, arguments, result, input_rows_count);
//        else
//            throw Exception("Illegal type " + block.get_by_position(arguments[0]).type->get_name() + " of argument of function " + get_name(),
//                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return Status::OK();
    }


    bool hasInformationAboutMonotonicity() const override
    {
        return true;
    }

//    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
//    {
//        IFunction::Monotonicity is_monotonic { true };
//        IFunction::Monotonicity is_not_monotonic;
//
//        if (std::is_same_v<typename Transform::FactorTransform, ZeroTransform>)
//        {
//            is_monotonic.is_always_monotonic = true;
//            return is_monotonic;
//        }
//
//        /// This method is called only if the function has one argument. Therefore, we do not care about the non-local time zone.
//        const DateLUTImpl & date_lut = DateLUT::instance();
//
//        if (left.isNull() || right.is_null())
//            return is_not_monotonic;
//
//        /// The function is monotonous on the [left, right] segment, if the factor transformation returns the same values for them.
//
//        if (checkAndGetDataType<DataTypeDate>(&type))
//        {
//            return Transform::FactorTransform::execute(UInt16(left.get<UInt64>()), date_lut)
//                == Transform::FactorTransform::execute(UInt16(right.get<UInt64>()), date_lut)
//                ? is_monotonic : is_not_monotonic;
//        }
//        else
//        {
//            return Transform::FactorTransform::execute(UInt32(left.get<UInt64>()), date_lut)
//                == Transform::FactorTransform::execute(UInt32(right.get<UInt64>()), date_lut)
//                ? is_monotonic : is_not_monotonic;
//        }
//    }
};

}

