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

//#include <IO/WriteHelpers.h>

#include "vec/columns/column_const.h"

#include "vec/columns/columns_common.h"
#include "vec/common/pod_array.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

ColumnConst::ColumnConst(const ColumnPtr& data_, size_t s_) : data(data_), s(s_) {
    /// Squash Const of Const.
    while (const ColumnConst* const_data = typeid_cast<const ColumnConst*>(data.get()))
        data = const_data->getDataColumnPtr();

    if (data->size() != 1)
        throw Exception("Incorrect size of nested column in constructor of ColumnConst: " +
                                std::to_string(data->size()) + ", must be 1.",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
}

ColumnPtr ColumnConst::convertToFullColumn() const {
    return data->replicate(Offsets(1, s));
}

ColumnPtr ColumnConst::removeLowCardinality() const {
    return ColumnConst::create(data->convertToFullColumnIfLowCardinality(), s);
}

ColumnPtr ColumnConst::filter(const Filter& filt, ssize_t /*result_size_hint*/) const {
    if (s != filt.size())
        throw Exception("Size of filter (" + std::to_string(filt.size()) +
                                ") doesn't match size of column (" + std::to_string(s) + ")",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, countBytesInFilter(filt));
}

ColumnPtr ColumnConst::replicate(const Offsets& offsets) const {
    if (s != offsets.size())
        throw Exception("Size of offsets (" + std::to_string(offsets.size()) +
                                ") doesn't match size of column (" + std::to_string(s) + ")",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    size_t replicated_size = 0 == s ? 0 : offsets.back();
    return ColumnConst::create(data, replicated_size);
}

ColumnPtr ColumnConst::permute(const Permutation& perm, size_t limit) const {
    if (limit == 0)
        limit = s;
    else
        limit = std::min(s, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation (" + std::to_string(perm.size()) +
                                ") is less than required (" + std::to_string(limit) + ")",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    return ColumnConst::create(data, limit);
}

//ColumnPtr ColumnConst::index(const IColumn & indexes, size_t limit) const
//{
//    if (limit == 0)
//        limit = indexes.size();
//
//    if (indexes.size() < limit)
//        throw Exception("Size of indexes (" + std::to_string(indexes.size()) + ") is less than required (" + std::to_string(limit) + ")",
//                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
//
//    return ColumnConst::create(data, limit);
//}

MutableColumns ColumnConst::scatter(ColumnIndex num_columns, const Selector& selector) const {
    if (s != selector.size())
        throw Exception("Size of selector (" + std::to_string(selector.size()) +
                                ") doesn't match size of column (" + std::to_string(s) + ")",
                        ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<size_t> counts = countColumnsSizeInSelector(num_columns, selector);

    MutableColumns res(num_columns);
    for (size_t i = 0; i < num_columns; ++i) res[i] = cloneResized(counts[i]);

    return res;
}

void ColumnConst::getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & res) const
{
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;
}

} // namespace doris::vectorized
