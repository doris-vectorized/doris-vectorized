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

#include "vec/olap/vcollect_iterator.h"

#include "olap/reader.h"
#include "olap/rowset/beta_rowset_reader.h"

namespace doris {
namespace vectorized {

void VCollectIterator::init(Reader* reader) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for better performance
    if (_reader->_reader_type == READER_QUERY &&
        (_reader->_aggregation || _reader->_tablet->keys_type() == KeysType::DUP_KEYS)) {
        _merge = false;
    }
}

OLAPStatus VCollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<LevelIterator> child(new Level0Iterator(rs_reader, _reader));
    _children.push_back(child.release());
    return OLAP_SUCCESS;
}

// Build a merge heap. If _merge is true, a rowset with the max rownum
// status will be used as the base rowset, and the other rowsets will be merged first and
// then merged with the base rowset.
void VCollectIterator::build_heap(const std::vector<RowsetReaderSharedPtr>& rs_readers) {
    DCHECK(rs_readers.size() == _children.size());
    _reverse = _reader->_tablet->tablet_schema().keys_type() == KeysType::UNIQUE_KEYS;
    if (_children.empty()) {
        _inner_iter.reset(nullptr);
        return;
    } else if (_merge) {
        DCHECK(!rs_readers.empty());
        for (auto& child : _children) { child->init(); }
        // build merge heap with two children, a base rowset as level0iterator and
        // other cumulative rowsets as a level1iterator
        if (_children.size() > 1) {
            // find 'base rowset', 'base rowset' is the rowset which contains the max row number
            int64_t max_row_num = 0;
            int base_reader_idx = 0;
            for (size_t i = 0; i < rs_readers.size(); ++i) {
                int64_t cur_row_num = rs_readers[i]->rowset()->rowset_meta()->num_rows();
                if (cur_row_num > max_row_num) {
                    max_row_num = cur_row_num;
                    base_reader_idx = i;
                }
            }
            auto base_reader_child = _children.begin();
            std::advance(base_reader_child, base_reader_idx);

            std::list<LevelIterator*> cumu_children;
            int i = 0;
            for (const auto& child : _children) {
                if (i != base_reader_idx) {
                    cumu_children.push_back(child);
                }
                ++i;
            }
            Level1Iterator* cumu_iter =
                    new Level1Iterator(cumu_children, _reader, cumu_children.size() > 1, _reverse);
            cumu_iter->init();
            std::list<LevelIterator*> children;
            children.push_back(*base_reader_child);
            children.push_back(cumu_iter);
            _inner_iter.reset(new Level1Iterator(children, _reader, _merge, _reverse));
        } else {
            // _children.size() == 1
            _inner_iter.reset(new Level1Iterator(_children, _reader, _merge, _reverse));
        }
    } else {
        _inner_iter.reset(new Level1Iterator(_children, _reader, _merge, _reverse));
    }
    _inner_iter->init();
    // Clear _children earlier to release any related references
    _children.clear();
}

bool VCollectIterator::LevelIteratorComparator::operator()(const LevelIterator* lhs,
                                                           const LevelIterator* rhs) {
    const Block* lhs_block;
    const Block* rhs_block;
    uint32_t lhs_id;
    uint32_t rhs_id;
    RETURN_NOT_OK(lhs->current_row(&lhs_block, &lhs_id));
    RETURN_NOT_OK(rhs->current_row(&rhs_block, &rhs_id));
    int cmp_res = lhs_block->compare_at(lhs_id, rhs_id, lhs->tablet_schema().num_key_columns(),
                                        *rhs_block, -1);
    if (cmp_res != 0) {
        return cmp_res > 0;
    }

    if (lhs->tablet_schema().has_sequence_col()) {
        int32_t sequence_col_idx = lhs->tablet_schema().sequence_col_idx();
        cmp_res = lhs_block->get_by_position(sequence_col_idx)
                          .column->compare_at(
                                  lhs_id, rhs_id,
                                  *(rhs_block->get_by_position(sequence_col_idx).column), -1);
        if (cmp_res != 0) {
            return cmp_res > 0;
        }
    }
    // if row cursors equal, compare data version.
    // read data from higher version to lower version.
    // for UNIQUE_KEYS just read the highest version and no need agg_update.
    // for AGG_KEYS if a version is deleted, the lower version no need to agg_update
    if (_reverse) {
        return lhs->version() < rhs->version();
    }
    return lhs->version() > rhs->version();
}

OLAPStatus VCollectIterator::current_row(const Block** block, uint32_t* row) const {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->current_row(block, row);
    }
    return OLAP_ERR_DATA_ROW_BLOCK_ERROR;
}

OLAPStatus VCollectIterator::next(const Block** block, uint32_t* row) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(block, row);
    } else {
        return OLAP_ERR_DATA_EOF;
    }
}

OLAPStatus VCollectIterator::next(Block* block) {
    if (LIKELY(_inner_iter)) {
        return _inner_iter->next(block);
    } else {
        return OLAP_ERR_DATA_EOF;
    }
}

VCollectIterator::Level0Iterator::Level0Iterator(RowsetReaderSharedPtr rs_reader, Reader* reader)
        : _rs_reader(rs_reader), _reader(reader), _current_row(0) {
    DCHECK_EQ(RowsetReader::BETA, rs_reader->type());
}

OLAPStatus VCollectIterator::Level0Iterator::init() {
    return _refresh_current_row();
}

OLAPStatus VCollectIterator::Level0Iterator::current_row(const Block** block, uint32_t* row) const {
    *row = _current_row;
    *block = &_block;
    return OLAP_SUCCESS;
}

int64_t VCollectIterator::Level0Iterator::version() const {
    return _rs_reader->version().second;
}

OLAPStatus VCollectIterator::Level0Iterator::_refresh_current_row() {
    do {
        if (_block.rows() != 0 && _current_row < _block.rows()) {
            return OLAP_SUCCESS;
        } else {
            auto res = _rs_reader->next_block(&_block);
            if (res != OLAP_SUCCESS) {
                _current_row = 0;
                return res;
            }
        }
    } while (_block.rows() != 0);
    _current_row = 0;
    return OLAP_ERR_DATA_EOF;
}

OLAPStatus VCollectIterator::Level0Iterator::next(const Block** block, uint32_t* row) {
    ++_current_row;
    RETURN_NOT_OK(_refresh_current_row());
    *row = _current_row;
    *block = &_block;
    return OLAP_SUCCESS;
}

OLAPStatus VCollectIterator::Level0Iterator::next(Block* block) {
    return _rs_reader->next_block(block);
}

const TabletSchema& VCollectIterator::Level0Iterator::tablet_schema() const {
    return _reader->tablet()->tablet_schema();
}

VCollectIterator::Level1Iterator::Level1Iterator(
        const std::list<VCollectIterator::LevelIterator*>& children, Reader* reader, bool merge,
        bool reverse)
        : _children(children), _reader(reader), _merge(merge), _reverse(reverse) {}

VCollectIterator::LevelIterator::~LevelIterator() {}

VCollectIterator::Level1Iterator::~Level1Iterator() {
    for (auto child : _children) {
        if (child != nullptr) {
            delete child;
            child = nullptr;
        }
    }
}

// Read next row into *row.
// Returns
//      OLAP_SUCCESS when read successfully.
//      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
//      Others when error happens
OLAPStatus VCollectIterator::Level1Iterator::next(const Block** block, uint32_t* row) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return OLAP_ERR_DATA_EOF;
    }
    if (_merge) {
        return _merge_next(block, row);
    } else {
        DCHECK(false) << "should not use this method.";
        return _normal_next(block, row);
    }
}

// Read next block
// Returns
//      OLAP_SUCCESS when read successfully.
//      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
//      Others when error happens
OLAPStatus VCollectIterator::Level1Iterator::next(Block* block) {
    if (UNLIKELY(_cur_child == nullptr)) {
        return OLAP_ERR_DATA_EOF;
    }
    return _normal_next(block);
}

// Get top row of the heap, nullptr if reach end.
OLAPStatus VCollectIterator::Level1Iterator::current_row(const Block** block, uint32_t* row) const {
    if (_cur_child != nullptr) {
        return _cur_child->current_row(block, row);
    }
    return OLAP_ERR_DATA_EOF;
}

int64_t VCollectIterator::Level1Iterator::version() const {
    if (_cur_child != nullptr) {
        return _cur_child->version();
    }
    return -1;
}

OLAPStatus VCollectIterator::Level1Iterator::init() {
    if (_children.empty()) {
        return OLAP_SUCCESS;
    }

    // Only when there are multiple children that need to be merged
    if (_merge && _children.size() > 1) {
        _heap.reset(new MergeHeap(LevelIteratorComparator(_reverse)));
        for (auto child : _children) {
            DCHECK(child != nullptr);
            //DCHECK(child->current_row() == OLAP_SUCCESS);
            _heap->push(child);
        }
        _cur_child = _heap->top();
        // Clear _children earlier to release any related references
        _children.clear();
    } else {
        _merge = false;
        _heap.reset(nullptr);
        _cur_child = *(_children.begin());
    }
    return OLAP_SUCCESS;
}

OLAPStatus VCollectIterator::Level1Iterator::_merge_next(const Block** block, uint32_t* row) {
    _heap->pop();
    auto res = _cur_child->next(block, row);
    if (LIKELY(res == OLAP_SUCCESS)) {
        _heap->push(_cur_child);
        _cur_child = _heap->top();
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        if (!_heap->empty()) {
            _cur_child = _heap->top();
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
    return _cur_child->current_row(block, row);
}

OLAPStatus VCollectIterator::Level1Iterator::_normal_next(const Block** block, uint32_t* row) {
    auto res = _cur_child->next(block, row);
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _cur_child->next(block, row);
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

OLAPStatus VCollectIterator::Level1Iterator::_normal_next(Block* block) {
    auto res = _cur_child->next(block);
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS;
    } else if (res == OLAP_ERR_DATA_EOF) {
        // current child has been read, to read next
        delete _cur_child;
        _children.pop_front();
        if (!_children.empty()) {
            _cur_child = *(_children.begin());
            return _cur_child->next(block);
        } else {
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        _cur_child = nullptr;
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

const TabletSchema& VCollectIterator::Level1Iterator::tablet_schema() const {
    return _reader->tablet()->tablet_schema();
}

} // namespace vectorized
} // namespace doris
