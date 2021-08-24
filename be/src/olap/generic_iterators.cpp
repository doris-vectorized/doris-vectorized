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
//      AutoIncrementIterator iter(schema, 1000);
//      StorageReadOptions opts;
//      RETURN_IF_ERROR(iter.init(opts));
//      RowBlockV2 block;
//      do {
//          st = iter.next_batch(&block);
//      } while (st.ok());
class AutoIncrementIterator : public RowwiseIterator {
public:
    // Will generate num_rows rows in total
    AutoIncrementIterator(const Schema& schema, size_t num_rows)
            : _schema(schema), _num_rows(num_rows), _rows_returned(0) {}
    ~AutoIncrementIterator() override {}

    // NOTE: Currently, this function will ignore StorageReadOptions
    Status init(const StorageReadOptions& opts) override;
    Status next_batch(RowBlockV2* block) override;

    Status next_batch(vectorized::Block* block) override {
        return Status::NotSupported("to be implemented. (TODO)");
    }

    const Schema& schema() const override { return _schema; }

private:
    Schema _schema;
    size_t _num_rows;
    size_t _rows_returned;
};

Status AutoIncrementIterator::init(const StorageReadOptions& opts) {
    return Status::OK();
}

Status AutoIncrementIterator::next_batch(RowBlockV2* block) {
    int row_idx = 0;
    while (row_idx < block->capacity() && _rows_returned < _num_rows) {
        RowBlockRow row = block->row(row_idx);

        for (int i = 0; i < _schema.num_columns(); ++i) {
            row.set_is_null(i, false);
            const auto* col_schema = _schema.column(i);
            switch (col_schema->type()) {
            case OLAP_FIELD_TYPE_SMALLINT:
                *(int16_t*)row.cell_ptr(i) = _rows_returned + i;
                break;
            case OLAP_FIELD_TYPE_INT:
                *(int32_t*)row.cell_ptr(i) = _rows_returned + i;
                break;
            case OLAP_FIELD_TYPE_BIGINT:
                *(int64_t*)row.cell_ptr(i) = _rows_returned + i;
                break;
            case OLAP_FIELD_TYPE_FLOAT:
                *(float*)row.cell_ptr(i) = _rows_returned + i;
                break;
            case OLAP_FIELD_TYPE_DOUBLE:
                *(double*)row.cell_ptr(i) = _rows_returned + i;
                break;
            default:
                break;
            }
        }
        row_idx++;
        _rows_returned++;
    }
    block->set_num_rows(row_idx);
    block->set_selected_size(row_idx);
    block->set_delete_state(DEL_PARTIAL_SATISFIED);
    if (row_idx > 0) {
        return Status::OK();
    }
    return Status::EndOfFile("End of AutoIncrementIterator");
}

// Used to store merge state for a MergeIterator input.
// This class will iterate all data from internal iterator
// through client call advance().
// Usage:
//      MergeIteratorContext ctx(iter);
//      RETURN_IF_ERROR(ctx.init());
//      while (ctx.valid()) {
//          visit(ctx.current_row());
//          RETURN_IF_ERROR(ctx.advance());
//      }
class MergeIteratorContext {
public:
    MergeIteratorContext(RowwiseIterator* iter, std::shared_ptr<MemTracker> parent)
            : _iter(iter), _block(iter->schema(), 1024, std::move(parent)) {}

    MergeIteratorContext(const MergeIteratorContext&) = delete;
    MergeIteratorContext(MergeIteratorContext&&) = delete;
    MergeIteratorContext& operator=(const MergeIteratorContext&) = delete;
    MergeIteratorContext& operator=(MergeIteratorContext&&) = delete;

    ~MergeIteratorContext() {
        delete _iter;
        _iter = nullptr;
    }

    // Initialize this context and will prepare data for current_row()
    Status init(const StorageReadOptions& opts);
    Status v_init(const StorageReadOptions& opts);

    bool compare(const MergeIteratorContext &rhs) const {
        const Schema& schema = _iter->schema();
        int num = schema.num_key_columns();
        for (uint32_t cid = 0; cid < num; ++cid) {
            auto name = schema.column(cid)->name();
            vectorized::ColumnWithTypeAndName l_col = _v_block.get_by_name(name);
            vectorized::ColumnWithTypeAndName r_col = rhs._v_block.get_by_name(name);

            vectorized::ColumnPtr l_cp = l_col.column;
            vectorized::ColumnPtr r_cp = r_col.column;

            vectorized::Field l_field = (*l_cp)[_v_index_in_block];
            vectorized::Field r_field = (*r_cp)[_v_index_in_block];

            if (l_field != r_field) {
                return (l_field < r_field) ? false : true;
            }
        }

        return this->data_id() < rhs.data_id();
    }

    void copy_row_to(vectorized::Block *block) {
        vectorized::Block &src = _v_block;
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
            vectorized::Field field = (*s_cp)[_v_index_in_block];
            ((vectorized::IColumn&)(*d_cp)).insert(field);
        }
    }

    // Return current row which internal row index points to
    // And this function won't make internal index advance.
    // Before call this function, Client must assure that
    // valid() return true
    RowBlockRow current_row() const {
        uint16_t* selection_vector = _block.selection_vector();
        return RowBlockRow(&_block, selection_vector[_index_in_block]);
    }

    // Advance internal row index to next valid row
    // Return error if error happens
    // Don't call this when valid() is false, action is undefined
    Status advance();
    Status v_advance();

    // Return if has remaining data in this context.
    // Only when this function return true, current_row()
    // will return a valid row
    bool valid() const { return _valid; }
    bool v_valid() const { return _v_valid; }

    int is_partial_delete() const { return _block.delete_state() == DEL_PARTIAL_SATISFIED; }

    uint64_t data_id() const { return _iter->data_id(); }

private:
    // Load next block into _block
    Status _load_next_block();
    Status _v_load_next_block();

private:
    RowwiseIterator* _iter;

    // used to store data load from iterator
    RowBlockV2 _block;

    bool _valid = false;
    size_t _index_in_block = -1;

    // used to store data load from iteerator->next_batch(Vectorized::Block*)
    vectorized::Block _v_block;

    bool _v_valid = false;
    size_t _v_index_in_block = -1;
};

Status MergeIteratorContext::init(const StorageReadOptions& opts) {
    RETURN_IF_ERROR(_iter->init(opts));
    RETURN_IF_ERROR(_load_next_block());
    if (valid()) {
        RETURN_IF_ERROR(advance());
    }
    return Status::OK();
}

Status MergeIteratorContext::v_init(const StorageReadOptions& opts) {
    RETURN_IF_ERROR(_iter->init(opts));
    RETURN_IF_ERROR(_v_load_next_block());
    if (v_valid()) {
        RETURN_IF_ERROR(v_advance());
    }
    return Status::OK();
}

Status MergeIteratorContext::advance() {
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _index_in_block++;
        if (_index_in_block < _block.selected_size()) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_load_next_block());
    } while (_valid);
    return Status::OK();
}

Status MergeIteratorContext::v_advance() {
    // NOTE: we increase _index_in_block directly to valid one check
    do {
        _v_index_in_block++;
        if (_v_index_in_block < _v_block.rows()) {
            return Status::OK();
        }
        // current batch has no data, load next batch
        RETURN_IF_ERROR(_v_load_next_block());
    } while (_v_valid);
    return Status::OK();
}

Status MergeIteratorContext::_load_next_block() {
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
    } while (_block.num_rows() == 0);
    _index_in_block = -1;
    _valid = true;
    return Status::OK();
}

Status MergeIteratorContext::_v_load_next_block() {
    do {
        _v_block.clear();
        Status st = _iter->next_batch(&_v_block);
        if (!st.ok()) {
            _v_valid = false;
            if (st.is_end_of_file()) {
                return Status::OK();
            } else {
                return st;
            }
        }
    } while (_v_block.rows() == 0);
    _v_index_in_block = -1;
    _v_valid = true;
    return Status::OK();
}

class MergeIterator : public RowwiseIterator {
public:
    // MergeIterator takes the ownership of input iterators
    MergeIterator(std::vector<RowwiseIterator*> &iters, std::shared_ptr<MemTracker> parent) : _origin_iters(std::move(iters)) {
        // use for count the mem use of Block use in Merge
        _mem_tracker = MemTracker::CreateTracker(-1, "MergeIterator", parent, false);
    }

    ~MergeIterator() override {
        while (!_merge_heap.empty()) {
            auto ctx = _merge_heap.top();
            _merge_heap.pop();
            delete ctx;
        }

        while (!_v_merge_heap.empty()) {
            auto ctx = _v_merge_heap.top();
            _v_merge_heap.pop();
            delete ctx;
        }
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(RowBlockV2* block) override;

    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

private:
    // It will be released after '_merge_heap' has been built.
    std::vector<RowwiseIterator*> _origin_iters;

    std::unique_ptr<Schema> _schema;

    struct MergeContextComparator {
        bool operator()(const MergeIteratorContext* lhs, const MergeIteratorContext* rhs) const {
            auto lhs_row = lhs->current_row();
            auto rhs_row = rhs->current_row();
            int cmp_res = compare_row(lhs_row, rhs_row);
            if (cmp_res != 0) {
                return cmp_res > 0;
            }
            // if row cursors equal, compare segment id.
            // here we sort segment id in reverse order, because of the row order in AGG_KEYS
            // dose no matter, but in UNIQUE_KEYS table we only read the latest is one, so we
            // return the row in reverse order of segment id
            return lhs->data_id() < rhs->data_id();
        }
    };

    struct VMergeContextComparator {
        bool operator()(const MergeIteratorContext* lhs, const MergeIteratorContext* rhs) const {
            return lhs->compare(*rhs);
        }
    };

    using MergeHeap = std::priority_queue<MergeIteratorContext*, 
                                        std::vector<MergeIteratorContext*>,
                                        MergeContextComparator>;

    MergeHeap _merge_heap;

    using VMergeHeap = std::priority_queue<MergeIteratorContext*, 
                                        std::vector<MergeIteratorContext*>,
                                        VMergeContextComparator>;

    VMergeHeap _v_merge_heap;
};

Status MergeIterator::init(const StorageReadOptions& opts) {
    if (_origin_iters.empty()) {
        return Status::OK();
    }
    _schema.reset(new Schema((*(_origin_iters.begin()))->schema()));

    for (auto iter : _origin_iters) {
        std::unique_ptr<MergeIteratorContext> ctx(new MergeIteratorContext(iter, _mem_tracker));

        if (opts.use_v_scan) {
            RETURN_IF_ERROR(ctx->v_init(opts));
            if (!ctx->v_valid()) {
                continue;
            }
            _v_merge_heap.push(ctx.release());
        } else {
            RETURN_IF_ERROR(ctx->init(opts));
            if (!ctx->valid()) {
                continue;
            }
            _merge_heap.push(ctx.release());
        }
    }

    _origin_iters.clear();
    return Status::OK();
}

Status MergeIterator::next_batch(RowBlockV2* block) {
    size_t row_idx = 0;
    for (; row_idx < block->capacity() && !_merge_heap.empty(); ++row_idx) {
        auto ctx = _merge_heap.top();
        _merge_heap.pop();

        RowBlockRow dst_row = block->row(row_idx);
        // copy current row to block
        copy_row(&dst_row, ctx->current_row(), block->pool());

        // TODO(hkp): refactor conditions and filter rows here with delete conditions
        if (ctx->is_partial_delete()) {
            block->set_delete_state(DEL_PARTIAL_SATISFIED);
        }
        RETURN_IF_ERROR(ctx->advance());
        if (ctx->valid()) {
            _merge_heap.push(ctx);
        } else {
            // Release ctx earlier to reduce resource consumed
            delete ctx;
        }
    }
    block->set_num_rows(row_idx);
    block->set_selected_size(row_idx);
    if (row_idx > 0) {
        return Status::OK();
    } else {
        return Status::EndOfFile("End of MergeIterator");
    }
}

Status MergeIterator::next_batch(vectorized::Block* block) {
    static const int block_row_max = 4096;

    while (block->rows() < block_row_max) {
        if (_v_merge_heap.empty())
            break;

        auto ctx = _v_merge_heap.top();
        _v_merge_heap.pop();

        // copy current row to block
        ctx->copy_row_to(block);

        RETURN_IF_ERROR(ctx->v_advance());
        if (ctx->v_valid()) {
            _v_merge_heap.push(ctx);
        } else {
            // Release ctx earlier to reduce resource consumed
            delete ctx;
        }
    }

    return Status::EndOfFile("no more data in segment");
}

// UnionIterator will read data from input iterator one by one.
class UnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transfered to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators any more.
    UnionIterator(std::vector<RowwiseIterator*> &v, std::shared_ptr<MemTracker> parent)
            : _origin_iters(v.begin(), v.end()) {
        _mem_tracker = MemTracker::CreateTracker(-1, "UnionIterator", parent, false);
    }

    ~UnionIterator() override {
        for (auto iter : _origin_iters) {
            delete iter;
        }
    }

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(RowBlockV2* block) override;
    Status next_batch(vectorized::Block* block) override;

    const Schema& schema() const override { return *_schema; }

private:
    std::unique_ptr<Schema> _schema;
    RowwiseIterator* _cur_iter = nullptr;
    std::deque<RowwiseIterator*> _origin_iters;
};

Status UnionIterator::init(const StorageReadOptions& opts) {
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

Status UnionIterator::next_batch(RowBlockV2* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is_end_of_file()) {
            delete _cur_iter;
            _cur_iter = nullptr;
            _origin_iters.pop_front();
            if (!_origin_iters.empty()) {
                _cur_iter = *(_origin_iters.begin());
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of UnionIterator");
}

Status UnionIterator::next_batch(vectorized::Block* block) {
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
    return Status::EndOfFile("End of UnionIterator");
}


RowwiseIterator* new_merge_iterator(std::vector<RowwiseIterator*>& inputs, std::shared_ptr<MemTracker> parent) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new MergeIterator(inputs, parent);
}

RowwiseIterator* new_union_iterator(std::vector<RowwiseIterator*>& inputs, std::shared_ptr<MemTracker> parent) {
    if (inputs.size() == 1) {
        return *(inputs.begin());
    }
    return new UnionIterator(inputs, parent);
}

RowwiseIterator* new_auto_increment_iterator(const Schema& schema, size_t num_rows) {
    return new AutoIncrementIterator(schema, num_rows);
}

} // namespace doris
