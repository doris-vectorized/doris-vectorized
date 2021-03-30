#pragma once
#include "runtime/runtime_state.h"
#include "vec/Functions/IFunction.h"
#include "vec/exprs/vexpr.h"

namespace DB {
class VSlotRef final : public VExpr {
public:
    VSlotRef(const doris::TExprNode& node);
    virtual doris::Status execute(DB::Block* block, int* result_column_id);
    virtual doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                  VExprContext* context);
    virtual VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VSlotRef(*this));
    }

private:
    FunctionPtr _function;
    int _slot_id;
    int _column_id;
    bool _is_nullable;
};
} // namespace DB
