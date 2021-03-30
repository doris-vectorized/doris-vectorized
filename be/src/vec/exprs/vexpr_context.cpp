#include "vec/exprs/vexpr_context.h"

#include "vec/exprs/vexpr.h"

namespace DB {
VExprContext::VExprContext(VExpr* expr)
        : _root(expr), _prepared(false), _opened(false), _closed(false) {}

doris::Status VExprContext::execute(DB::Block* block, int* result_column_id) {
    return _root->execute(block, result_column_id);
}

doris::Status VExprContext::prepare(doris::RuntimeState* state,
                                    const doris::RowDescriptor& row_desc,
                                    const std::shared_ptr<doris::MemTracker>& tracker) {
    return _root->prepare(state, row_desc, this);
}
doris::Status VExprContext::open(doris::RuntimeState* state) {
    return _root->open(state, this);
}
} // namespace DB
