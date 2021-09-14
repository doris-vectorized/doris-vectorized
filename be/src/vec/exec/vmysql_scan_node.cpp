#include "vec/exec/vmysql_scan_node.h"

#include "exec/text_converter.h"
#include "exec/text_converter_column.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/types.h"
namespace doris::vectorized {

VMysqlScanNode::VMysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.mysql_scan_node.table_name),
          _tuple_id(tnode.mysql_scan_node.tuple_id),
          _columns(tnode.mysql_scan_node.columns),
          _filters(tnode.mysql_scan_node.filters),
          _tuple_desc(nullptr),
          _slot_num(0) {}

VMysqlScanNode::~VMysqlScanNode() {}

Status VMysqlScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VMysqlScanNode::Prepare";
    if (_is_init) {
        return Status::OK();
    }

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    RETURN_IF_ERROR(ExecNode::prepare(state));
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (NULL == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }
    _slot_num = _tuple_desc->slots().size();
    const MySQLTableDescriptor* mysql_table =
            static_cast<const MySQLTableDescriptor*>(_tuple_desc->table_desc());

    if (NULL == mysql_table) {
        return Status::InternalError("mysql table pointer is NULL.");
    }
    _my_param.host = mysql_table->host();
    _my_param.port = mysql_table->port();
    _my_param.user = mysql_table->user();
    _my_param.passwd = mysql_table->passwd();
    _my_param.db = mysql_table->mysql_db();

    _mysql_scanner.reset(new (std::nothrow) MysqlScanner(_my_param));
    if (_mysql_scanner.get() == NULL) {
        return Status::InternalError("new a mysql scanner failed.");
    }

    _tuple_pool.reset(new (std::nothrow) MemPool(mem_tracker().get())); 
    if (_tuple_pool.get() == NULL) {
        return Status::InternalError("new a mem pool failed.");
    }

    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
    if (_text_converter.get() == NULL) {
        return Status::InternalError("new a text convertor failed.");
    }
    _is_init = true;
    return Status::OK();
}

Status VMysqlScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG_CRITICAL << "MysqlScanNode::Open";

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_mysql_scanner->open());
    RETURN_IF_ERROR(_mysql_scanner->query(_table_name, _columns, _filters, _limit));
    int materialize_num = 0;
    for (int i = 0; i < _tuple_desc->slots().size(); ++i) {
        if (_tuple_desc->slots()[i]->is_materialized()) {
            materialize_num++;
        }
    }
    if (_mysql_scanner->field_num() != materialize_num) {
        return Status::InternalError("input and output not equal.");
    }

    return Status::OK();
}

Status VMysqlScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VMysqlScanNode Node::get_next scalar");
}

Status VMysqlScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    VLOG_CRITICAL << "VMysqlScanNode::GetNext";
    if (state == NULL || block == NULL || eos == NULL)
        return Status::InternalError("input is NULL pointer");
    if (!_is_init) return Status::InternalError("used before initialize.");

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);

    std::vector<vectorized::MutableColumnPtr> columns;
    bool mysql_eos = false;

    while (true) {
        RETURN_IF_CANCELLED(state);
        int batch_size = state->batch_size();
        if (block->rows() == batch_size) {
            LOG(INFO) << "data full: " << batch_size;
            return Status::OK(); //TODO 如果数据满了？？？
        }

        char** data = NULL;
        unsigned long* length = NULL;
        RETURN_IF_ERROR(_mysql_scanner->get_next_row(&data, &length, &mysql_eos));

        if (mysql_eos) {
            *eos = true;
            return Status::OK();
        }
        int j = 0;
        for (int i = 0; i < _slot_num; ++i) {

            auto slot_desc = _tuple_desc->slots()[i];
            columns.emplace_back(slot_desc->get_empty_mutable_column());

            if (!slot_desc->is_materialized()) {
                continue;
            }
            if (data[j] == nullptr) {
                if (slot_desc->is_nullable()) {
                    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(columns[i].get());
                    nullable_column->insert_data(nullptr, 0);
                } else {
                    std::stringstream ss;
                    ss << "nonnull column contains NULL. table=" << _table_name
                       << ", column=" << slot_desc->col_name();
                    return Status::InternalError(ss.str());
                }
            } else {
                RETURN_IF_ERROR(write_text_slot(data[j], length[j], slot_desc, &columns[i], state));
            }
            j++;
        }
        auto n_columns = 0;
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert({columns[n_columns++]->get_ptr(), slot_desc->get_data_type_ptr(),
                           slot_desc->col_name()});
        }
    }
    return Status::OK();
}

Status VMysqlScanNode::write_text_slot(char* value, int value_length, SlotDescriptor* slot,
                                       vectorized::MutableColumnPtr* column_ptr,
                                       RuntimeState* state) {
    if (!_text_converter->write_column(slot, column_ptr, value, value_length, true, false,
                                       _tuple_pool.get())) {
        std::stringstream ss;
        ss << "Fail to convert mysql value:'" << value << "' to " << slot->type() << " on column:`"
           << slot->col_name() + "`";
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

Status VMysqlScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    _tuple_pool.reset();
    return ExecNode::close(state);
}
void VMysqlScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "MysqlScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

} // namespace doris::vectorized