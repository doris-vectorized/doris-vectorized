#include <thrift/protocol/TJSONProtocol.h>

#include <boost/shared_ptr.hpp>

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
} // namespace doris::vectorized

namespace apache::thrift {
template <typename ThriftStruct>
ThriftStruct from_json_string(const std::string& json_val) {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    ThriftStruct ts;
    TMemoryBuffer* buffer =
            new TMemoryBuffer((uint8_t*)json_val.c_str(), (uint32_t)json_val.size());
    boost::shared_ptr<TTransport> trans(buffer);
    TJSONProtocol protocol(trans);
    ts.read(&protocol);
    return ts;
}
} // namespace apache::thrift
