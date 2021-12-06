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

#include "olap/olap_define.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/reader.h"
#include "vec/core/block.h"

namespace doris {

class TabletSchema;

namespace vectorized {

class VCollectIterator {
public:
    // Hold reader point to get reader params
    void init(Reader* reader);

    OLAPStatus add_child(RowsetReaderSharedPtr rs_reader);

    void build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers);
    // Get top row of the heap, nullptr if reach end.
    OLAPStatus current_row(const Block** block, uint32_t* row) const;

    // Read nest order row in Block.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
    //      Others when error happens
    OLAPStatus next(const Block** block, uint32_t* row);

    OLAPStatus next(Block* block);

    bool is_merge() const { return _merge; };

private:
    // This interface is the actual implementation of the new version of iterator.
    // It currently contains two implementations, one is Level0Iterator,
    // which only reads data from the rowset reader, and the other is Level1Iterator,
    // which can read merged data from multiple LevelIterators through MergeHeap.
    // By using Level1Iterator, some rowset readers can be merged in advance and
    // then merged with other rowset readers.
    class LevelIterator {
    public:
        LevelIterator(Reader* reader): _schema(reader->tablet()->tablet_schema()) {};

        virtual OLAPStatus init() = 0;

        virtual int64_t version() const = 0;

        virtual OLAPStatus current_row(const Block** block, uint32_t* row) const = 0;

        virtual OLAPStatus next(const Block** block, uint32_t* row) = 0;

        virtual OLAPStatus next(Block* block) = 0;

        virtual ~LevelIterator() = default;

        const TabletSchema& tablet_schema() const {
            return _schema;
        };

        bool need_skip() const {
            return _skip_row;
        }

        void set_need_skip(bool skip) const {
            _skip_row = skip;
        }

        const TabletSchema& _schema;
        mutable bool _skip_row = false;
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class LevelIteratorComparator {
    public:
        LevelIteratorComparator(const bool reverse = false, int sequence = -1) : _reverse(reverse), _sequence(sequence) {}

        bool operator()(const LevelIterator* lhs, const LevelIterator* rhs);

    private:
        bool _reverse;
        int _sequence;
    };

    typedef std::priority_queue<LevelIterator*, std::vector<LevelIterator*>,
                                LevelIteratorComparator>
            MergeHeap;

    // Iterate from rowset reader. This Iterator usually like a leaf node
    class Level0Iterator : public LevelIterator {
    public:
        Level0Iterator(RowsetReaderSharedPtr rs_reader, Reader* reader);

        OLAPStatus init() override;

        int64_t version() const override;

        OLAPStatus current_row(const Block** block, uint32_t* row) const override;

        OLAPStatus next(const Block** block, uint32_t* row) override;

        OLAPStatus next(Block* block) override;

    private:
        OLAPStatus _refresh_current_row();

        RowsetReaderSharedPtr _rs_reader;
        Reader* _reader = nullptr;
        uint32_t _current_row = 0;
        Block _block;
    };

    // Iterate from LevelIterators (maybe Level0Iterators or Level1Iterator or mixed)
    class Level1Iterator : public LevelIterator {
    public:
        Level1Iterator(const std::list<LevelIterator*>& children, Reader* reader, bool merge, bool reverse);

        OLAPStatus init() override;

        int64_t version() const override;

        OLAPStatus current_row(const Block** block, uint32_t* row) const override;

        OLAPStatus next(const Block** block, uint32_t* row) override;

        OLAPStatus next(Block* block) override;

        ~Level1Iterator();

    private:
        inline OLAPStatus _merge_next(const Block** block, uint32_t* row);

        inline OLAPStatus _normal_next(const Block** block, uint32_t* row);

        inline OLAPStatus _normal_next(Block* block);

        // Each LevelIterator corresponds to a rowset reader,
        // it will be cleared after '_heap' has been initilized when '_merge == true'.
        std::list<LevelIterator*> _children;
        // point to the Level0Iterator containing the next output row.
        // null when VCollectIterator hasn't been initialized or reaches EOF.
        LevelIterator* _cur_child = nullptr;
        Reader* _reader = nullptr;

        // when `_merge == true`, rowset reader returns ordered rows and VCollectIterator uses a priority queue to merge
        // sort them. The output of VCollectIterator is also ordered.
        // When `_merge == false`, rowset reader returns *partial* ordered rows. VCollectIterator simply returns all rows
        // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
        // *partially* ordered.
        bool _merge = true;
        bool _reverse = false;
        // used when `_merge == true`
        std::unique_ptr<MergeHeap> _heap;
        // used when `_merge == false`
        int _child_idx = 0;
    };

    std::unique_ptr<LevelIterator> _inner_iter;

    // Each LevelIterator corresponds to a rowset reader,
    // it will be cleared after '_inner_iter' has been initilized.
    std::list<LevelIterator*> _children;

    bool _merge = true;
    bool _reverse = false;
    // Hold reader point to access read params, such as fetch conditions.
    Reader* _reader = nullptr;
};

} // namespace vectorized
} // namespace doris
