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

//#include <IO/WriteBufferFromString.h>
//#include <IO/Operators.h>
#include "vec/columns/column.h"

#include <sstream>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"

namespace doris::vectorized {

std::string IColumn::dumpStructure() const {
    std::stringstream res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dumpStructure();
    };

    const_cast<IColumn*>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn& src, size_t n) {
    insert(src[n]);
}

bool isColumnNullable(const IColumn& column) {
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn& column) {
    return checkColumn<ColumnConst>(column);
}

} // namespace doris::vectorized
