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

#include "vec/exprs/vectorized_fn_call.h"

#include "fmt/format.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_types_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
using doris::Status;

VectorizedFnCall::VectorizedFnCall(const doris::TExprNode& node) : VExpr(node) {}

doris::Status VectorizedFnCall::prepare(doris::RuntimeState* state,
                                        const doris::RowDescriptor& desc, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));
    _function = SimpleFunctionFactory::instance().get(_fn.name.function_name);
    if (_function == nullptr) {
        return Status::InternalError(
                fmt::format("Function {} is not implemented", _fn.name.function_name));
    }
    if (!_data_type->isNullable()) {
        _data_type = std::make_shared<DataTypeNullable>(_data_type);
    }
    return Status::OK();
}
doris::Status VectorizedFnCall::open(doris::RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));
    return Status::OK();
}
void VectorizedFnCall::close(doris::RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

Status VectorizedFnCall::execute(doris::vectorized::Block* block, int* result_column_id) {
    // for each child call execute
    doris::vectorized::ColumnNumbers arguments;
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        _children[i]->execute(block, &column_id);
        arguments.emplace_back(column_id);
    }
    // call function
    size_t num_columns_without_result = block->columns();
    // todo spec column name
    block->insert({ nullptr, _data_type, fmt::format("{}()",_fn.name.function_name)});
    _function->execute(*block, arguments, num_columns_without_result, block->rows(), false);
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

} // namespace doris::vectorized
