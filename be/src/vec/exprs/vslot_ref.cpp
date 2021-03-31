#include "vec/exprs/vslot_ref.h"

#include "fmt/format.h"
#include "runtime/descriptors.h"

namespace DB {
using doris::Status;
using doris::SlotDescriptor;
VSlotRef::VSlotRef(const doris::TExprNode& node)
        : VExpr(node), _slot_id(node.slot_ref.slot_id), _column_id(-1), _is_nullable(false) {}

Status VSlotRef::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                         VExprContext* context) {
    DCHECK_EQ(_children.size(), 0);
    if (_slot_id == -1) {
        return Status::OK();
    }
    const SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == NULL) {
        return Status::InternalError(fmt::format("couldn't resolve slot descriptor {}", _slot_id));
    }
    _is_nullable = slot_desc->is_nullable();
    _column_id = desc.get_column_id(_slot_id);
    return Status::OK();
}

doris::Status VSlotRef::execute(DB::Block* block, int* result_column_id) {
    DCHECK_GE(_column_id , 0);
    *result_column_id = _column_id;
    return Status::OK();
}

} // namespace DB
