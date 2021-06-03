
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

#include "vec/data_types/data_type_number.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function_date_or_datetime_to_something.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionYear = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToYearImpl>;
using FunctionQuarter = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToQuarterImpl>;
using FunctionMonth = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMonthImpl>;
using FunctionDay = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDayImpl>;
using FunctionHour = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToHourImpl>;
using FunctionMinute = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToMinuteImpl>;
using FunctionSecond = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToSecondImpl>;
using FunctionToDays = FunctionDateOrDateTimeToSomething<DataTypeInt32, ToDaysImpl>;

void registerFunctionToTimeFuction(SimpleFunctionFactory & factory)
{
    factory.registerFunction<FunctionSecond>();
    factory.registerFunction<FunctionMinute>();
    factory.registerFunction<FunctionHour>();
    factory.registerFunction<FunctionDay>();
    factory.registerFunction<FunctionMonth>();
    factory.registerFunction<FunctionYear>();
    factory.registerFunction<FunctionQuarter>();
    factory.registerFunction<FunctionToDays>();
}

}
