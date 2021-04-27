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

#include <queue>

#include "common/object_pool.h"
#include "util/tuple_row_compare.h"

#include "vec/core/sort_cursor.h"

namespace doris {

class RowBatch;
class RuntimeProfile;

namespace vectorized {
class Block;
// VSortedRunMerger is used to merge multiple sorted runs of tuples. A run is a sorted
// sequence of row batches, which are fetched from a BlockSupplier function object.
// Merging is implemented using a binary min-heap that maintains the run with the next
// tuple in sorted order at the top of the heap.
//
// Merged batches of rows are retrieved from VSortedRunMerger via calls to get_next().
// The merger is constructed with a boolean flag deep_copy_input.
// If true, sorted output rows are deep copied into the data pool of the output batch.
// If false, get_next() only copies tuple pointers (TupleRows) into the output batch,
// and transfers resource ownership from the input batches to the output batch when
// an input batch is processed.
class VSortedRunMerger {
public:
    // Function that returns the next batch of rows from an input sorted run. The batch
    // is owned by the supplier (i.e. not VSortedRunMerger). eos is indicated by an NULL
    // batch being returned.
    VSortedRunMerger(const std::vector<VExprContext *>& ordering_expr, const std::vector<bool>& _is_asc_order,
            const std::vector<bool>& _nulls_first, const size_t batch_size, int64_t limit, size_t offset, RuntimeProfile* profile);

    virtual ~VSortedRunMerger() = default;

    // Prepare this merger to merge and return rows from the sorted runs in 'input_runs'.
    // Retrieves the first batch from each run and sets up the binary heap implementing
    // the priority queue.
    Status prepare(const std::vector<BlockSupplier>& input_runs, bool parallel = false);

    // Return the next batch of sorted rows from this merger.
    Status get_next(Block* output_block, bool *eos);

    // Only Child class implement this Method, Return the next batch of sorted rows from this merger.
    virtual Status get_batch(RowBatch **output_batch) {
        return Status::InternalError("no support method get_batch(RowBatch** output_batch)");
    }

protected:
    const std::vector<VExprContext *>& _ordering_expr;
    const std::vector<bool>& _is_asc_order;
    const std::vector<bool>& _nulls_first;
    const size_t _batch_size;

    size_t _num_rows_returned = 0;
    int64_t _limit = -1;
    size_t _offset = 0;

    std::vector<ReceiveQueueSortCursorImpl> _cursors;
    std::priority_queue<SortCursor> _priority_queue;

    Block _empty_block;

    // Pool of BatchedRowSupplier instances.
    ObjectPool _pool;

    // Times calls to get_next().
    RuntimeProfile::Counter *_get_next_timer;

    // Times calls to get the next batch of rows from the input run.
    RuntimeProfile::Counter *_get_next_batch_timer;

private:
    void next_heap(SortCursor& current);
};

}
} // namespace doris

