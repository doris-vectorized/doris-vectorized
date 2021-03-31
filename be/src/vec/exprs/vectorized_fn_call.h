#pragma once
#include "runtime/runtime_state.h"
#include "vec/functions/function.h"
#include "vec/exprs/vexpr.h"

namespace DB {
class VectorizedFnCall final : public VExpr {
public:
    VectorizedFnCall(const doris::TExprNode& node);
    virtual doris::Status execute(DB::Block* block, int* result_column_id);
    virtual doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                  VExprContext* context);
    virtual doris::Status open(doris::RuntimeState* state, VExprContext* context);
    virtual void close(doris::RuntimeState* state, VExprContext* context);
    virtual VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VectorizedFnCall(*this));
    }

private:
    FunctionPtr _function;
};
} // namespace DB
