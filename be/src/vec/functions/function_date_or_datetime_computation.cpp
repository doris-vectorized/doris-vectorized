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

#include "vec/functions/function_date_or_datetime_computation.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

using FunctionAddSeconds = FunctionDateOrDateTimeComputation<AddSecondsImpl>;
using FunctionAddMinutes = FunctionDateOrDateTimeComputation<AddMinutesImpl>;
using FunctionAddHours = FunctionDateOrDateTimeComputation<AddHoursImpl>;
using FunctionAddDays = FunctionDateOrDateTimeComputation<AddDaysImpl>;
using FunctionAddWeeks = FunctionDateOrDateTimeComputation<AddWeeksImpl>;
using FunctionAddMonths = FunctionDateOrDateTimeComputation<AddMonthsImpl>;
using FunctionAddQuarters = FunctionDateOrDateTimeComputation<AddQuartersImpl>;
using FunctionAddYears = FunctionDateOrDateTimeComputation<AddYearsImpl>;

using FunctionSubSeconds = FunctionDateOrDateTimeComputation<SubtractSecondsImpl>;
using FunctionSubMinutes = FunctionDateOrDateTimeComputation<SubtractMinutesImpl>;
using FunctionSubHours = FunctionDateOrDateTimeComputation<SubtractHoursImpl>;
using FunctionSubDays = FunctionDateOrDateTimeComputation<SubtractDaysImpl>;
using FunctionSubWeeks = FunctionDateOrDateTimeComputation<SubtractWeeksImpl>;
using FunctionSubMonths = FunctionDateOrDateTimeComputation<SubtractMonthsImpl>;
using FunctionSubQuarters = FunctionDateOrDateTimeComputation<SubtractQuartersImpl>;
using FunctionSubYears = FunctionDateOrDateTimeComputation<SubtractYearsImpl>;

using FunctionDateDiff = FunctionDateOrDateTimeComputation<DateDiffImpl>;
using FunctionTimeDiff = FunctionDateOrDateTimeComputation<TimeDiffImpl>;
using FunctionYearsDiff = FunctionDateOrDateTimeComputation<YearsDiffImpl>;
using FunctionMonthsDiff = FunctionDateOrDateTimeComputation<MonthsDiffImpl>;
using FunctionDaysDiff = FunctionDateOrDateTimeComputation<DaysDiffImpl>;
using FunctionWeeksDiff = FunctionDateOrDateTimeComputation<WeeksDiffImpl>;
using FunctionHoursDiff = FunctionDateOrDateTimeComputation<HoursDiffImpl>;
using FunctionMinutesDiff = FunctionDateOrDateTimeComputation<MintueSDiffImpl>;
using FunctionSecondsDiff = FunctionDateOrDateTimeComputation<SecondsDiffImpl>;

void registerFunctionDateTimeComputation(SimpleFunctionFactory& factory) {
    factory.registerFunction<FunctionAddSeconds>();
    factory.registerFunction<FunctionAddMinutes>();
    factory.registerFunction<FunctionAddHours>();
    factory.registerFunction<FunctionAddDays>();
    factory.registerFunction<FunctionAddWeeks>();
    factory.registerFunction<FunctionAddMonths>();
    factory.registerFunction<FunctionAddYears>();
    factory.registerFunction<FunctionAddQuarters>();

    factory.registerFunction<FunctionSubSeconds>();
    factory.registerFunction<FunctionSubMinutes>();
    factory.registerFunction<FunctionSubHours>();
    factory.registerFunction<FunctionSubDays>();
    factory.registerFunction<FunctionSubMonths>();
    factory.registerFunction<FunctionSubYears>();
    factory.registerFunction<FunctionSubQuarters>();
    factory.registerFunction<FunctionSubWeeks>();

    factory.registerFunction<FunctionDateDiff>();
    factory.registerFunction<FunctionTimeDiff>();
    factory.registerFunction<FunctionYearsDiff>();
    factory.registerFunction<FunctionMonthsDiff>();
    factory.registerFunction<FunctionWeeksDiff>();
    factory.registerFunction<FunctionDaysDiff>();
    factory.registerFunction<FunctionHoursDiff>();
    factory.registerFunction<FunctionMinutesDiff>();
    factory.registerFunction<FunctionSecondsDiff>();
}

} // namespace doris::vectorized