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

#include "vec/data_types/data_type.h"

#include "fmt/format.h"
#include "common/logging.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/exception.h"
#include "vec/data_types/nested_utils.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int MULTIPLE_STREAMS_REQUIRED;
extern const int LOGICAL_ERROR;
extern const int DATA_TYPE_CANNOT_BE_PROMOTED;
} // namespace ErrorCodes

IDataType::IDataType() {}

IDataType::~IDataType() {}

String IDataType::get_name() const {
    return doGetName();
}

String IDataType::doGetName() const {
    return get_family_name();
}

void IDataType::updateAvgValueSizeHint(const IColumn& column, double& avg_value_size_hint) {
    /// Update the average value size hint if amount of read rows isn't too small
    size_t column_size = column.size();
    if (column_size > 10) {
        double current_avg_value_size = static_cast<double>(column.byte_size()) / column_size;

        /// Heuristic is chosen so that avg_value_size_hint increases rapidly but decreases slowly.
        if (current_avg_value_size > avg_value_size_hint)
            avg_value_size_hint = std::min(1024., current_avg_value_size); /// avoid overestimation
        else if (current_avg_value_size * 2 < avg_value_size_hint)
            avg_value_size_hint = (current_avg_value_size + avg_value_size_hint * 3) / 4;
    }
}

ColumnPtr IDataType::createColumnConst(size_t size, const Field& field) const {
    auto column = createColumn();
    column->insert(field);
    return ColumnConst::create(std::move(column), size);
}

ColumnPtr IDataType::createColumnConstWithDefaultValue(size_t size) const {
    return createColumnConst(size, getDefault());
}

DataTypePtr IDataType::promoteNumericType() const {
    throw Exception("Data type " + get_name() + " can't be promoted.",
                    ErrorCodes::DATA_TYPE_CANNOT_BE_PROMOTED);
}

size_t IDataType::getSizeOfValueInMemory() const {
    throw Exception("Value of type " + get_name() + " in memory is not of fixed size.",
                    ErrorCodes::LOGICAL_ERROR);
}

void IDataType::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    LOG(FATAL) << fmt::format("Data type {} to_string not implement.", get_name());
}

std::string IDataType::to_string(const IColumn& column, size_t row_num) const {
    LOG(FATAL) << fmt::format("Data type {} to_string not implement.", get_name());
}

void IDataType::insertDefaultInto(IColumn& column) const {
    column.insert_default();
}

} // namespace doris::vectorized
