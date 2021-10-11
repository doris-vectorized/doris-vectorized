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
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/core/materialize_block.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris {
namespace vectorized {

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
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    int _probe_index = -1;
    DataTypes _left_table_data_types;

    template <class HashTableContext>
    friend class HashTableProbeIntersect;
};
} // namespace vectorized
} // namespace doris