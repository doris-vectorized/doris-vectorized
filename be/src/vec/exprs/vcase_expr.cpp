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

#include "vec/exprs/vcase_expr.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
 
VCaseExpr::VCaseExpr(const TExprNode& node)
    : VExpr(node),
        _has_case_expr(node.case_expr.has_case_expr),
        _has_else_expr(node.case_expr.has_else_expr) {}
 
doris::Status VCaseExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                  VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));
    return Status::OK();
}
 
doris::Status VCaseExpr::open(doris::RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));
    // data type follows then column's type
    _data_type = _has_case_expr ? _children[2]->data_type() : _children[1]->data_type();
    return Status::OK();
}
 
void VCaseExpr::close(doris::RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

doris::Status VCaseExpr::_execute_generic(doris::vectorized::Block* block, int* result_column_id) {
    std::vector<size_t> arguments(_children.size());
    int input_row_count = 0;

    // convert all column to nullable for later compare and insert, exclude `case when expr`
    for (int i = 0; i < _children.size(); i++) {
        int column_id = -1;
        _children[i]->execute(block, &column_id);
        arguments[i] = column_id;
        
        // if column is column const, need to convert full column for later compare and insert
        block->replace_by_position_if_const(column_id);
        auto child_column = block->get_by_position(column_id).column;
        input_row_count = child_column->size();

        if (!child_column->is_nullable()) {
            auto bool_column = ColumnUInt8::create();
            bool_column->insert_many_defaults(input_row_count);

            arguments[i] = block->columns();
            block->insert({doris::vectorized::ColumnNullable::create(child_column, bool_column->get_ptr()),
                std::make_shared<DataTypeNullable>(block->get_by_position(column_id).type),
                "case when nullable"
            });
        }
    }
    
    auto else_column_ptr = _has_else_expr ? block->get_by_position(arguments[arguments.size() - 1]).column : nullptr;
    // make return column
    MutableColumnPtr ret_column_ptr = _data_type->create_column();
    if (!_data_type->is_nullable()) {
        ret_column_ptr = doris::vectorized::ColumnNullable::create(std::move(ret_column_ptr), doris::vectorized::ColumnUInt8::create());
    }
    auto* tmp_ret_column_ptr = reinterpret_cast<ColumnNullable*>(ret_column_ptr.get());

    // stands for the pos of the last then column
    int end_pos = arguments.size() - 1;
    if (_has_else_expr) {
        end_pos--; // exclude else column
    }

    if (_has_case_expr) {
        auto case_column_ptr = block->get_by_position(arguments[0]).column;
 
        for (int row_idx = 0; row_idx < input_row_count; row_idx++) {
            for (int arg_idx = 1; arg_idx < end_pos; arg_idx = arg_idx + 2) {
                auto when_column_ptr = block->get_by_position(arguments[arg_idx]).column;
                
                if (case_column_ptr->is_null_at(row_idx)) {
                    if (else_column_ptr == nullptr) {
                        tmp_ret_column_ptr->insert_default();
                    } else {
                        tmp_ret_column_ptr->insert_from(*else_column_ptr, row_idx);
                    }
                    break;
                }

                if (when_column_ptr->is_null_at(row_idx)) {
                    if (arg_idx == end_pos - 1) {
                        if (else_column_ptr == nullptr) {
                            tmp_ret_column_ptr->insert_default();
                        } else {
                            tmp_ret_column_ptr->insert_from(*else_column_ptr, row_idx);
                        }
                        break;
                    }
                    continue;
                }

                if (case_column_ptr->compare_at(row_idx, row_idx, *when_column_ptr, -1) == 0) {
                    auto then_column_ptr = block->get_by_position(arguments[arg_idx + 1]).column;
                    tmp_ret_column_ptr->insert_from(*then_column_ptr, row_idx);
                    break;
                }

                if (arg_idx == end_pos - 1) {
                    if (else_column_ptr == nullptr) {
                        tmp_ret_column_ptr->insert_default();
                    } else {
                        tmp_ret_column_ptr->insert_from(*else_column_ptr, row_idx);
                    }
                    break;
                }
            }
        }
    } else {
        for (int row_idx = 0; row_idx < input_row_count; row_idx++) {
            for (int arg_idx = 0; arg_idx < end_pos; arg_idx = arg_idx + 2) {
                auto column_ptr = block->get_by_position(arguments[arg_idx]).column;
                if (column_ptr->is_null_at(row_idx)) {
                    if (arg_idx == end_pos - 1) {
                        if (else_column_ptr == nullptr) {
                            tmp_ret_column_ptr->insert_default();
                        } else {
                            tmp_ret_column_ptr->insert_from(*else_column_ptr, row_idx);
                        }
                        break;
                    }
                    continue;
                }
                
                auto* case_when_null_column_ptr =  reinterpret_cast<const doris::vectorized::ColumnNullable*>(column_ptr.get());
                auto* case_when_column_ptr = check_and_get_column<doris::vectorized::ColumnVector<UInt8>>(case_when_null_column_ptr->get_nested_column());
                if (!case_when_column_ptr) {
                    return Status::InvalidArgument("case when expr required type columnVector<Uint8>");
                }
                if ((*case_when_column_ptr)[row_idx] == 1u) {
                    auto then_column_ptr = block->get_by_position(arguments[arg_idx + 1]).column;
                    tmp_ret_column_ptr->insert_from(*then_column_ptr, row_idx);
                    break;
                }

                if (arg_idx == end_pos - 1) {
                    if (else_column_ptr == nullptr) {
                        tmp_ret_column_ptr->insert_default();
                    } else {
                        tmp_ret_column_ptr->insert_from(*else_column_ptr, row_idx);
                    }
                    break;
                }
            }
        }
    }

    *result_column_id = block->columns();
    if (!_data_type->is_nullable()) {
        block->insert({ret_column_ptr->get_ptr(), std::make_shared<DataTypeNullable>(_data_type), "vcase expr result colummn"});
    } else {
        block->insert({ret_column_ptr->get_ptr(), _data_type, "vcase expr result colummn"});   
    }

    return Status::OK();
}
 
 doris::Status VCaseExpr::execute(doris::vectorized::Block* block, int* result_column_id) {
    // todo(wb) add type check here
    return _execute_generic(block, result_column_id);

    // todo(wb) using code-gen to eliminate branch and make tight loop for SIMD 
}

const std::string& VCaseExpr::expr_name() const {
    return _expr_name;
}
 
} // namespace