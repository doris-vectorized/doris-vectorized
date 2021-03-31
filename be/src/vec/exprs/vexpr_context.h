#pragma once

#include "vec/core/block.h"
#include "common/status.h"
#include "runtime/runtime_state.h"

namespace DB {
class VExpr;


class VExprContext {
public:
    VExprContext(VExpr *expr);
    doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& row_desc,
                   const std::shared_ptr<doris::MemTracker>& tracker);
    doris::Status open(doris::RuntimeState* state);
    void close(doris::RuntimeState* state);
    doris::Status execute(DB::Block* block, int *result_column_id);
private:
    VExpr *_root;
    bool _prepared;
    bool _opened;
    bool _closed;
};
}