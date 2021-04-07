#pragma once

#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/vexpr.h"

namespace doris {
class TExprNode;

namespace vectorized {
class VLiteral : public VExpr {
public:
    virtual ~VLiteral();
    VLiteral(const TExprNode& node);
    virtual Status execute(vectorized::Block* block, int* result_column_id) override;
    virtual const std::string& expr_name() const override { return _expr_name; }
    virtual VExpr* clone(doris::ObjectPool* pool) const override {
        return pool->add(new VLiteral(*this));
    }
private:
    ColumnPtr _column_ptr;
    std::string _expr_name;
};
} // namespace vectorized

} // namespace doris
