# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/vec")
# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/vec")

set(VEC_FILES
  data_types/nested_utils.cpp
  data_types/data_type.cpp
  data_types/data_type_number_base.cpp
  data_types/data_types_number.cpp
  common/demangle.cpp
  functions/function.cpp
  functions/function_helpers.cpp
  columns/column_nullable.cpp
  columns/column_vector.cpp
  columns/column_const.cpp
  columns/column.cpp
  columns/columns_common.cpp
  core/field.cpp
  core/column_with_type_and_name.cpp
  core/block_info.cpp
  core/block.cpp
  common/pod_array.cpp
  common/string_utils/string_utils.cpp
  common/error_codes.cpp
  common/exception.cpp
  aggregate_functions/aggregate_function_sum.cpp
  columns/collator.cpp
  columns/column_decimal.cpp
  columns/column_string.cpp
  core/field.cpp
  data_types/data_types_decimal.cpp
  data_types/data_type_string.cpp
  exprs/vectorized_fn_call.cpp
  exprs/vexpr.cpp
  exprs/vexpr_context.cpp
  exprs/vectorized_agg_fn.cpp
  exprs/vslot_ref.cpp
  exec/aggregation_node.cpp
  data_types/data_type_nullable.cpp
  data_types/data_type_nothing.cpp
)

add_library(Vec STATIC
    ${VEC_FILES}
)
