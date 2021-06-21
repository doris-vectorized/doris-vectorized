#include "runtime/datetime_value.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct StrToDate {
    static constexpr auto name = "str_to_date";
    using ReturnType = DataTypeDateTime;
    using ColumnType = ColumnVector<Int128>;

    static void vector_vector(const ColumnString::Chars& ldata,
                              const ColumnString::Offsets& loffsets,
                              const ColumnString::Chars& rdata,
                              const ColumnString::Offsets& roffsets, ColumnType::Container& res,
                              NullMap& null_map) {
        size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            const char* l_raw_str = reinterpret_cast<const char*>(&ldata[loffsets[i - 1]]);
            int l_str_size = loffsets[i] - loffsets[i - 1] - 1;

            const char* r_raw_str = reinterpret_cast<const char*>(&rdata[roffsets[i - 1]]);
            int r_str_size = roffsets[i] - roffsets[i - 1] - 1;

            auto& ts_val = *reinterpret_cast<DateTimeValue*>(&res[i]);
            if (!ts_val.from_date_format_str(r_raw_str, r_str_size, l_raw_str, l_str_size)) {
                null_map[i] = 1;
            }
            ts_val.to_datetime();
        }
    }
};

using FunctionStrToDate = FunctionBinaryStringOperateToNullType<StrToDate>;

void register_function_str_to_date(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStrToDate>();
}

} // namespace doris::vectorized