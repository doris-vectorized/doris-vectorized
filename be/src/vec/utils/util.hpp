#include "runtime/descriptors.h"
#include "vec/core/block.h"

namespace doris::vectorized {
class VectorizedUtils {
public:
    static Block create_empty_columnswithtypename(const RowDescriptor& row_desc) {
        // Block block;
        doris::vectorized::ColumnsWithTypeAndName columns_with_type_and_name;
        for (const auto tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto slot_desc : tuple_desc->slots()) {
                columns_with_type_and_name.emplace_back(nullptr, slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name());
            }
        }
        return columns_with_type_and_name;
    }
};

} // namespace dois::vectorized
