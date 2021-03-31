// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <vector>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

class VExpr {
public:
    VExpr(const TExprNode& node);
    virtual VExpr* clone(ObjectPool* pool) const = 0;
    // VExpr(const VExpr& expr);
    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                                  VExprContext* context);
    virtual void close(RuntimeState* state, VExprContext* context);
    virtual Status open(RuntimeState* state, VExprContext* context);
    virtual Status execute(vectorized::Block* block, int* result_column_id) = 0;

    void add_child(VExpr* expr) { _children.push_back(expr); }

    static Status create_expr_tree(ObjectPool* pool, const TExpr& texpr,
                                          VExprContext** ctx);

    bool is_nullable() { return _data_type->isNullable(); }

    static Status create_expr(ObjectPool* pool, const TExprNode& texpr_node,
                                     VExpr** expr);

    static Status create_tree_from_thrift(ObjectPool* pool,
                                                 const std::vector<TExprNode>& nodes,
                                                 VExpr* parent, int* node_idx, VExpr** root_expr,
                                                 VExprContext** ctx);

protected:
    TExprNodeType::type _node_type;
    TypeDescriptor _type;
    DataTypePtr _data_type;
    std::vector<VExpr*> _children;
    TFunction _fn;
};

} // namespace doris::vectorized
