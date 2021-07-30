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

#include "vec/exprs/vexpr.h"

#include <fmt/format.h>

#include "gen_cpp/Exprs_types.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {
using doris::Status;
using doris::RuntimeState;
using doris::RowDescriptor;
using doris::TypeDescriptor;

VExpr::VExpr(const doris::TExprNode& node)
        : _node_type(node.node_type),
          _type(TypeDescriptor::from_thrift(node.type)),
          _data_type(_type.get_data_type_ptr()) {
    if (node.__isset.fn) {
        _fn = node.fn;
    }
}

Status VExpr::prepare(RuntimeState* state, const RowDescriptor& row_desc, VExprContext* context) {
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, row_desc, context));
    }
    return Status::OK();
}

Status VExpr::open(RuntimeState* state, VExprContext* context) {
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->open(state, context));
    }
    return Status::OK();
}

void VExpr::close(doris::RuntimeState* state, VExprContext* context) {
    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->close(state, context);
    }
}

Status VExpr::create_expr(doris::ObjectPool* pool, const doris::TExprNode& texpr_node,
                          VExpr** expr) {
    switch (texpr_node.node_type) {
    case doris::TExprNodeType::SLOT_REF: {
        *expr = pool->add(new VSlotRef(texpr_node));
        break;
    }
    case doris::TExprNodeType::FUNCTION_CALL: {
        *expr = pool->add(new VectorizedFnCall(texpr_node));
        break;
    }
    default:
        return Status::InternalError(
                fmt::format("Unknown expr node type: {}", texpr_node.node_type));
    }
    return Status::OK();
}

Status VExpr::create_tree_from_thrift(doris::ObjectPool* pool,
                                      const std::vector<doris::TExprNode>& nodes, VExpr* parent,
                                      int* node_idx, VExpr** root_expr, VExprContext** ctx) {
    // propagate error case
    if (*node_idx >= nodes.size()) {
        return Status::InternalError("Failed to reconstruct expression tree from thrift.");
    }
    int num_children = nodes[*node_idx].num_children;
    VExpr* expr = nullptr;
    RETURN_IF_ERROR(create_expr(pool, nodes[*node_idx], &expr));
    DCHECK(expr != nullptr);
    if (parent != nullptr) {
        parent->add_child(expr);
    } else {
        DCHECK(root_expr != nullptr);
        DCHECK(ctx != nullptr);
        *root_expr = expr;
        *ctx = pool->add(new VExprContext(expr));
    }
    for (int i = 0; i < num_children; i++) {
        *node_idx += 1;
        RETURN_IF_ERROR(create_tree_from_thrift(pool, nodes, expr, node_idx, nullptr, nullptr));
        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= nodes.size()) {
            return Status::InternalError("Failed to reconstruct expression tree from thrift.");
        }
    }
    return Status::OK();
}

Status VExpr::create_expr_tree(doris::ObjectPool* pool, const doris::TExpr& texpr,
                               VExprContext** ctx) {
    if (texpr.nodes.size() == 0) {
        *ctx = nullptr;
        return Status::OK();
    }
    int node_idx = 0;
    VExpr* e = nullptr;
    Status status = create_tree_from_thrift(pool, texpr.nodes, NULL, &node_idx, &e, ctx);
    if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
        status = Status::InternalError(
                "Expression tree only partially reconstructed. Not all thrift nodes were used.");
    }
    if (!status.ok()) {
        LOG(ERROR) << "Could not construct expr tree.\n"
                   << status.get_error_msg() << "\n"
                   << apache::thrift::ThriftDebugString(texpr);
    }
    return Status::OK();
}

Status VExpr::create_expr_trees(ObjectPool* pool, const std::vector<doris::TExpr>& texprs,
                               std::vector<VExprContext*>* ctxs) {
    ctxs->clear();
    for (int i = 0; i < texprs.size(); ++i) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(create_expr_tree(pool, texprs[i], &ctx));
        ctxs->push_back(ctx);
    }
    return Status::OK();
}

Status VExpr::prepare(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                     const RowDescriptor& row_desc, const std::shared_ptr<MemTracker>& tracker) {
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->prepare(state, row_desc, tracker));
    }
    return Status::OK();
}

void VExpr::close(const std::vector<VExprContext*>& ctxs, RuntimeState* state) {
    for (int i = 0; i < ctxs.size(); ++i) {
        ctxs[i]->close(state);
    }
}

Status VExpr::open(const std::vector<VExprContext*>& ctxs, RuntimeState* state) {
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->open(state));
    }
    return Status::OK();
}

} // namespace doris::vectorized
