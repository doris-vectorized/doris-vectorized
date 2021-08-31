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

#include <queue>
#include <utility>

#include "olap/iterators.h"
#include "olap/row.h"
#include "olap/row_block2.h"
#include "olap/row_cursor_cell.h"

namespace doris {

// This iterator will generate ordered data. For example for schema
// (int, int) this iterator will generator data like
// (0, 1), (1, 2), (2, 3), (3, 4)...
//
// Usage:
//      Schema schema;
//      VAutoIncrementIterator iter(schema, 1000);
//      StorageReadOptions opts;
//      RETURN_IF_ERROR(iter.init(opts));
//      RowBlockV2 block;
//      do {
//          st = iter.next_batch(&block);
//      } while (st.ok());
class VAutoIncrementIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    VAutoIncrementIterator(const Schema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows), _rows_returned(0) {}
    ~VAutoIncrementIterator() override {}

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override;

    Status next_batch(vectorized::Block* block) override {
        return Status::NotSupported("to be implemented. (TODO)");
    }

    const Schema& schema() const override { return _schema; }

private:
    Schema _schema;
    size_t _num_rows;
    size_t _rows_returned;
};

Status VAutoIncrementIterator::init(const StorageReadOptions& opts) {
    return Status::OK();
}

// Used to store merge state for a VMergeIterator input.
// This class will iterate all data from internal iterator
// through client call advance().
// Usage:
//      VMergeIteratorContext ctx(iter);
//      RETURN_IF_ERROR(ctx.init());
//      while (ctx.valid()) {
//          visit(ctx.current_row());
//          RETURN_IF_ERROR(ctx.advance());
//      }
class VMergeIteratorContext {
public:
    VMergeIteratorContext(RowwiseIterator* iter, std::shared_ptr<MemTracker> parent) : _iter(iter) {}
    VMergeIteratorContext(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext(VMergeIteratorContext&&) = delete;
    VMergeIteratorContext& operator=(const VMergeIteratorContext&) = delete;
    VMergeIteratorContext& operator=(VMergeIteratorContext&&) = delete;

    ~VMergeIteratorContext() {
        delete _iter;
        _iter = nullptr;
    }

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);

    int compare_row(const VMergeIteratorContext &rhs) const {
        const Schema& schema = _iter->schema();
        int num = schema.num_key_columns();
        for (uint32_t cid = 0; cid < num; ++cid) {
            auto name = schema.column(cid)->name();
            auto l_col = this->_block.get_by_name(name);
            auto r_col = rhs._block.get_by_name(name);

            auto l_cp = l_col.column;
            auto r_cp = r_col.column;

            auto res = l_cp->compare_at(_index_in_block, _index_in_block, *r_cp, -1);
            if (res) {
                return res;
            }
        }

        return 0;
    }

    bool compare(const VMergeIteratorContext &rhs) const {
        int cmp_res = this->compare_row(rhs);
        if (cmp_res != 0) {
            return cmp_res > 0;
        }
        return this->data_id() < rhs.data_id();
    }

    void copy_row_to(vectorized::Block *block) {
        vectorized::Block &src = _block;
        vectorized::Block &dst = *block;

        auto columns = _iter->schema().columns();
        assert(columns.size() == src.columns());
        assert(src.size() == dst.columns());

        for (size_t i = 0; i < columns.size(); ++i) {
            vectorized::ColumnWithTypeAndName s_col = src.get_by_position(i);
            vectorized::ColumnWithTypeAndName d_col = dst.get_by_position(i);

            vectorized::ColumnPtr s_cp = s_col.column;
            vectorized::ColumnPtr d_cp = d_col.column;

            //copy a row to dst block column by column
            vectorized::Field field = (*s_cp)[_index_in_block];
            ((vectorized::IColumn&)(*d_cp)).insert(field);
        }
    }

    // Advance internal row index to next valid row
    // Return error if error happens
    // Don't call this when valid() is false, action is undefined
    Status advance();

    // Return if has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    bool valid() const { return _valid; }

    uint64_t data_id() const { return _iter->data_id(); }

private:
    // Load next block into _block
    Status _load_next_block();

private:
    RowwiseIterator* _iter;

    // used to store data load from iteerator->next_batch(Vectorized::Block*)
    vectorized::Block _block;

    bool _valid = false;
    size_t _index_in_block = -1;
};

Status VMergeIteratorContext::init(const StorageReadOptions& opts) {
    RETURN_IF_ERROR(_iter->init(opts));
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(advance());
    }
    return Status::OK();
}

Status VMergeIteratorContext::advance() {
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (_index_in_block < _block.rows()) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status VMergeIteratorContext::_load_next_block() {
    do {
        _block.clear();
        Status st = _iter->next_batch(&_block);
        if (!st.ok()) {
            _valid = false;
            if (st.is_end_of_file()) {
                return Status::OK();
            } else {
                return st;
            }
        }
    } while (_block.rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

class VMergeIterator : public RowwiseIterator {
public:
    // VMergeIterator takes the ownership of input iterators
    VMergeIterator(std::vector<RowwiseIterator*> &iters, std::shared_ptr<MemTracker> parent) : _origin_iters(std::move(iters)) {
        // use for count the mem use of Block use in Merge
        _mem_tracker = MemTracker::CreateTracker(-1, "VMergeIterator", parent, false);
    }

    ~VMergeIterator() override {
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            delete ctx;
        }
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

private:
    // It will be released after '_merge_heap' has been built.
    std::vector<RowwiseIterator*> _origin_iters;

    std::unique_ptr<Schema> _schema;

    struct VMergeContextComparator {
        bool operator()(const VMergeIteratorContext* lhs, const VMergeIteratorContext* rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using VMergeHeap = std::priority_queue<VMergeIteratorContext*, 
                                        std::vector<VMergeIteratorContext*>,
                                        VMergeContextComparator>;

    VMergeHeap _merge_heap;
};

Status VMergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema.reset(new Schema((*(_origin_iters.begin()))->schema()));

    for (auto iter : _origin_iters) {
        std::unique_ptr<VMergeIteratorContext> ctx(new VMergeIteratorContext(iter, _mem_tracker));
        RETURN_IF_ERROR(ctx->init(opts));
        if (!ctx->valid()) {
            continue;
        }
        _merge_heap.push(ctx.release());
    }

    _origin_iters.clear();
    return Status::OK();
}

Status VMergeIterator::next_batch(vectorized::Block* block) {
    static const int block_row_max = 4096;

    while (block->rows() < block_row_max) {
        if (_merge_heap.empty())
            break;

        auto ctx = _merge_heap.top();
        _merge_heap.pop();

        // copy current row to block
        ctx->copy_row_to(block);

        RETURN_IF_ERROR(ctx->advance());
        if (ctx->valid()) {
            _merge_heap.push(ctx);
        } else {
            // Release ctx earlier to reduce resource consumed
            delete ctx;
        }
    }

    return Status::EndOfFile("no more data in segment");
}

// VUnionIterator will read data from input iterator one by one.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transfered to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators any more.
    VUnionIterator(std::vector<RowwiseIterator*> &v, std::shared_ptr<MemTracker> parent)
            : _origin_iters(v.begin(), v.end()) {
        _mem_tracker = MemTracker::CreateTracker(-1, "VUnionIterator", parent, false);
    }

    ~VUnionIterator() override {
        std::for_each(_origin_iters.begin(), _origin_iters.end(), std::default_delete<RowwiseIterator>());
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

private:
    std::unique_ptr<Schema> _schema;
    RowwiseIterator* _cur_iter = nullptr;
    std::deque<RowwiseIterator*> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }

    for (auto iter : _origin_iters) {
        RETURN_IF_ERROR(iter->init(opts));
    }
    _schema.reset(new Schema((*(_origin_iters.begin()))->schema()));
    _cur_iter = *(_origin_iters.begin());
    return Status::OK();
}

Status VUnionIterator::next_batch(vectorized::Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is_end_of_file()) {
            delete _cur_iter;
            _origin_iters.pop_front();
            if (!_origin_iters.empty()) {
                _cur_iter = *(_origin_iters.begin());
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}


RowwiseIterator* new_merge_iterator(std::vector<RowwiseIterator*>& inputs, std::shared_ptr<MemTracker> parent) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new VMergeIterator(inputs, parent);
}

RowwiseIterator* new_union_iterator(std::vector<RowwiseIterator*>& inputs, std::shared_ptr<MemTracker> parent) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new VUnionIterator(inputs, parent);
}

RowwiseIterator* new_auto_increment_iterator(const Schema& schema, size_t num_rows) {
    return new VAutoIncrementIterator(schema, num_rows);
}

} // namespace doris
