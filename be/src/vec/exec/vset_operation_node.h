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

#include "codegen/doris_ir.h"
#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/core/materialize_block.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"
#include "vec/exec/join/vhash_join_node.h"

namespace doris {

namespace vectorized {

class VSetOperationNode : public ExecNode {
public:
    VSetOperationNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
        return Status::NotSupported("Not Implemented get RowBatch in vecorized execution.");
    }
    virtual Status close(RuntimeState* state);
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

    virtual void   hash_table_init();
    virtual Status hash_table_build(RuntimeState* state);    
    virtual Status process_build_block(Block& block); 
    virtual Status extract_build_join_column(Block& block, ColumnRawPtrs& raw_ptrs);
    virtual Status extract_probe_join_column(Block& block, ColumnRawPtrs& raw_ptrs);
protected:
    HashTableVariants _hash_table_variants;

    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _build_key_sz;

    std::vector<bool> _build_not_ignore_null;
    std::vector<bool> _probe_not_ignore_null;

    Arena _arena;
    AcquireList<Block> _acquire_list;
    bool _has_init_hash_table;
protected:
    /// Const exprs materialized by this node. These exprs don't refer to any children.
    /// Only materialized by the first fragment instance to avoid duplication.
    std::vector<std::vector<VExprContext*>> _const_expr_lists;

    /// Exprs materialized by this node. The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;

    /// Index of current const result expr list.
    int _const_expr_list_idx;

    /// Index of current child.
    int _child_idx;

    /// Index of current row in child_row_block_.
    int _child_row_idx;

    /// Saved from the last to GetNext() on the current child.
    bool _child_eos;

    /// Index of the child that needs to be closed on the next GetNext() call. Should be set
    /// to -1 if no child needs to be closed.
    int _to_close_child_idx;

    // Time spent to evaluates exprs and materializes the results
    RuntimeProfile::Counter* _materialize_exprs_evaluate_timer = nullptr;
    RuntimeProfile::Counter* _build_timer; // time to build hash table
    RuntimeProfile::Counter* _probe_timer; // time to probe

    template <class HashTableContext>
    friend class ProcessHashTableBuild2;
};

} // namespace vectorized
} // namespace doris
