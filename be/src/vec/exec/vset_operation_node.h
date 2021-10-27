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
#include "vec/core/materialize_block.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

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

protected:
    void hash_table_init();
    Status hash_table_build(RuntimeState* state);
    Status process_build_block(Block& block);
    Status extract_build_column(Block& block, ColumnRawPtrs& raw_ptrs);
    Status extract_probe_column(Block& block, ColumnRawPtrs& raw_ptrs, int child_id);
    template <bool keep_matched>
    void refresh_hash_table();

protected:
    HashTableVariants _hash_table_variants;

    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _build_key_sz;
    std::vector<bool> _build_not_ignore_null;

    Arena _arena;
    AcquireList<Block> _acquire_list;
    //record element size in hashtable
    int64_t _valid_element_in_hash_tbl;

    //The i-th result expr list refers to the i-th child.
    std::vector<std::vector<VExprContext*>> _child_expr_lists;
    //record build column type
    DataTypes _left_table_data_types;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> _build_col_idx;

    RuntimeProfile::Counter* _build_timer; // time to build hash table
    RuntimeProfile::Counter* _probe_timer; // time to probe

    template <class HashTableContext>
    friend class HashTableBuild;
};
template <bool keep_matched>
void VSetOperationNode::refresh_hash_table() {
    std::visit(
            [&](auto&& arg) {
                using HashTableCtxType = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                    HashTableCtxType tmp_hash_table;
                    bool is_need_shrink =
                            arg.hash_table.should_be_shrink(_valid_element_in_hash_tbl);
                    if (is_need_shrink) {
                        tmp_hash_table.hash_table.init_buf_size(
                                _valid_element_in_hash_tbl / arg.hash_table.get_factor() + 1);
                    }
                    arg.init_once();
                    auto& iter = arg.iter;
                    for (; iter != arg.hash_table.end(); ++iter) {
                        auto& mapped = iter->get_second();
                        auto it = mapped.begin();
                        if constexpr (keep_matched) {
                            if (it->visited) {
                                it->visited = false;
                                if (is_need_shrink)
                                    tmp_hash_table.hash_table.insert(iter->get_value());
                            } else {
                                arg.hash_table.delete_zero_key(iter->get_first());
                                iter->set_zero();
                                mapped.~RowRefList();
                            }
                        } else {
                            if (!it->visited && is_need_shrink) {
                                tmp_hash_table.hash_table.insert(iter->get_value());
                            }
                        }
                    }
                    arg.inited = false;
                    if (is_need_shrink) {
                        arg.hash_table = std::move(tmp_hash_table.hash_table);
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants);
}
} // namespace vectorized
} // namespace doris
