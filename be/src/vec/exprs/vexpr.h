#pragma once

#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/data_types/data_type.h"

namespace DB {

class VExpr {
public:
    VExpr(const doris::TExprNode& node);
    virtual VExpr* clone(doris::ObjectPool* pool) const = 0;
    // VExpr(const VExpr& expr);
    virtual doris::Status prepare(doris::RuntimeState* state, const doris::RowDescriptor& row_desc,
                                  VExprContext* context);
    virtual void close(doris::RuntimeState* state, VExprContext* context);
    virtual doris::Status open(doris::RuntimeState* state, VExprContext* context);
    virtual doris::Status execute(DB::Block* block, int* result_column_id) = 0;

    void add_child(VExpr*expr) {_children.push_back(expr);}

    static doris::Status create_expr_tree(doris::ObjectPool* pool, const doris::TExpr& texpr, VExprContext** ctx);
    
    static doris::Status create_expr(doris::ObjectPool* pool, const doris::TExprNode& texpr_node, VExpr** expr);
    static doris::Status create_tree_from_thrift(doris::ObjectPool* pool, const std::vector<doris::TExprNode>& nodes,
                                       VExpr* parent, int* node_idx, VExpr** root_expr,
                                       VExprContext** ctx);
protected:
    doris::TExprNodeType::type _node_type;
    doris::TypeDescriptor _type;
    DataTypePtr _data_type;
    std::vector<VExpr*> _children;
    doris::TFunction _fn;
};

} // namespace DB
