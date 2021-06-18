#pragma once

#include "vec/columns/column.h"
#include "vec/common/hash_table/hash_table_key_holder.h"

namespace doris::vectorized {

template <bool is_plain_column = false>
static auto getKeyHolder(const IColumn& column, size_t row_num, Arena& arena) {
    if constexpr (is_plain_column) {
        return ArenaKeyHolder{column.get_data_at(row_num), arena};
    } else {
        const char* begin = nullptr;
        StringRef serialized = column.serialize_value_into_arena(row_num, arena, begin);
        assert(serialized.data != nullptr);
        return SerializedKeyHolder{serialized, arena};
    }
}

template <bool is_plain_column>
static void deserializeAndInsert(StringRef str, IColumn& data_to) {
    if constexpr (is_plain_column)
        data_to.insert_data(str.data, str.size);
    else
        data_to.deserialize_and_insert_from_arena(str.data);
}

} // namespace doris::vectorized
