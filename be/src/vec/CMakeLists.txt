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
DataTypes/NestedUtils.cpp
DataTypes/IDataType.cpp
DataTypes/DataTypeNumberBase.cpp
DataTypes/DataTypesNumber.cpp
common/demangle.cpp
Functions/IFunction.cpp
Functions/FunctionHelpers.cpp
Columns/ColumnNullable.cpp
Columns/ColumnVector.cpp
Columns/ColumnConst.cpp
Columns/IColumn.cpp
Columns/ColumnsCommon.cpp
Core/Field.cpp
Core/ColumnWithTypeAndName.cpp
Core/BlockInfo.cpp
Core/Block.cpp
Common/PODArray.cpp
Common/StringUtils/StringUtils.cpp
Common/ErrorCodes.cpp
Common/Exception.cpp
AggregateFunctions/AggregateFunctionSum.cpp
           Columns/Collator.cpp
   Columns/ColumnDecimal.cpp
   Columns/ColumnString.cpp
   Core/Field.cpp
   DataTypes/DataTypesDecimal.cpp
        DataTypes/DataTypeString.cpp
exprs/vectorized_fn_call.cpp
exprs/vexpr.cpp
exprs/vexpr_context.cpp
exprs/vslot_ref.cpp

DataTypes/DataTypeNullable.cpp
DataTypes/DataTypeNothing.cpp
)

add_library(Vec STATIC
    ${VEC_FILES}
)
