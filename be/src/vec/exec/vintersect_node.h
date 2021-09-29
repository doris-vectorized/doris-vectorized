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

#include "exec/exec_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/core/materialize_block.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"
#include "vec/exec/join/vhash_join_node.h"
namespace doris {
namespace vectorized {

struct SerializedHashTableContext2 {
    using Mapped = RowRefList;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    //using Iter = typename HashTable::iterator;
    static constexpr auto could_handle_asymmetric_null = false;  //
    HashTable hash_table;
    // bool inited = false;

    // void init_once() {
    //     if (!inited) {
    //         inited = true;
    //         iter = hash_table.begin();
    //     }
    // }
};
//using HashTableVariants2 = std::variant<std::monostate, SerializedHashTableContext2>;
class VExprContext;

class VIntersectNode : public VSetOperationNode {
public:
    VIntersectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, vectorized::Block* output_block, bool* eos);
    virtual Status close(RuntimeState* state);

private:
    void debug_string(int indentation_level, std::stringstream* out) const;
    Status _hash_table_build(RuntimeState* state);
    Status _process_build_block(Block& block); 

    Status extract_build_join_column(Block& block,
                                  ColumnRawPtrs& raw_ptrs);

    Status extract_probe_join_column(Block& block, 
                                  ColumnRawPtrs& raw_ptrs);
    void _hash_table_init();
private:

    int64_t _hash_table_rows;
    Arena _arena;
    HashTableVariants _hash_table_variants;
    AcquireList<Block> _acquire_list;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;

    int _probe_index = -1;

    std::vector<bool> _build_not_ignore_null;
    std::vector<bool> _probe_not_ignore_null;
    std::vector<size_t> _probe_key_sz;
    std::vector<size_t> _build_key_sz;
    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;
    template <class HashTableContext>
    friend class ProcessHashTableBuild2;
    template <class HashTableContext>
    friend class ProcessHashTableProbe2;
};
} // namespace vectorized
} // namespace doris