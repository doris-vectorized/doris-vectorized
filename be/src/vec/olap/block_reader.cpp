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

#include "vec/olap/block_reader.h"

#include <parallel_hashmap/phmap.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_set>

#include "vec/olap/vcollect_iterator.h"
#include "olap/row_block.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/date_func.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris::vectorized {

BlockReader::BlockReader() : _collect_iter(new VCollectIterator()) {}

OLAPStatus BlockReader::_init_collect_iter(const ReaderParams& read_params,
        std::vector<RowsetReaderSharedPtr>* valid_rs_readers) {
    _collect_iter->init(this);
    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto res = _capture_rs_readers(read_params, &rs_readers);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    for (auto& rs_reader : rs_readers) {
        RETURN_NOT_OK(rs_reader->init(&_reader_context));
        OLAPStatus res = _collect_iter->add_child(rs_reader);
        if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to add child to iterator, err=" << res;
            return res;
        }
        if (res == OLAP_SUCCESS) {
            valid_rs_readers->push_back(rs_reader);
        }
    }
    _collect_iter->build_heap(*valid_rs_readers);

    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::init(const ReaderParams& read_params) {
    Reader::init(read_params);

    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto status = _init_collect_iter(read_params, &rs_readers);
    if (status != OLAP_SUCCESS) { return status; }

    if (_optimize_for_single_rowset(rs_readers)) {
        _next_block_func = _tablet->keys_type() == AGG_KEYS ? &BlockReader::_direct_agg_key_next_block
                                                          : &BlockReader::_direct_next_block;
        return OLAP_SUCCESS;
    }

    switch (_tablet->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_block_func = &BlockReader::_direct_next_block;
        break;
    case KeysType::UNIQUE_KEYS:
        _next_block_func = &BlockReader::_unique_key_next_block;
        break;
    case KeysType::AGG_KEYS:
        _next_block_func = &BlockReader::_agg_key_next_block;
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet->keys_type();
        break;
    }

    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_direct_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                    bool* eof) {
    auto res = _collect_iter->next(block);
    if (UNLIKELY(res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF)) {
        return res;
    }
    *eof = res == OLAP_ERR_DATA_EOF;
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_direct_agg_key_next_block(Block* block, MemPool* mem_pool,
                                            ObjectPool* agg_pool, bool* eof) {
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_agg_key_next_block(Block* block, MemPool* mem_pool, ObjectPool* agg_pool,
                                     bool* eof) {
    return OLAP_SUCCESS;
}

OLAPStatus BlockReader::_unique_key_next_block(Block* block, MemPool* mem_pool,
                                        ObjectPool* agg_pool, bool* eof) {
    return OLAP_SUCCESS;
}

} // namespace doris
