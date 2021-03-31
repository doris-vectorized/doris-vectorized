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

#include "vec/data_types/data_types_number.h"
// #include <vec/DataTypes/DataTypeFactory.h>

namespace doris::vectorized {

// void registerDataTypeNumbers(DataTypeFactory & factory)
// {
//     factory.registerSimpleDataType("UInt8", [] { return DataTypePtr(std::make_shared<DataTypeUInt8>()); });
//     factory.registerSimpleDataType("UInt16", [] { return DataTypePtr(std::make_shared<DataTypeUInt16>()); });
//     factory.registerSimpleDataType("UInt32", [] { return DataTypePtr(std::make_shared<DataTypeUInt32>()); });
//     factory.registerSimpleDataType("UInt64", [] { return DataTypePtr(std::make_shared<DataTypeUInt64>()); });

//     factory.registerSimpleDataType("Int8", [] { return DataTypePtr(std::make_shared<DataTypeInt8>()); });
//     factory.registerSimpleDataType("Int16", [] { return DataTypePtr(std::make_shared<DataTypeInt16>()); });
//     factory.registerSimpleDataType("Int32", [] { return DataTypePtr(std::make_shared<DataTypeInt32>()); });
//     factory.registerSimpleDataType("Int64", [] { return DataTypePtr(std::make_shared<DataTypeInt64>()); });

//     factory.registerSimpleDataType("Float32", [] { return DataTypePtr(std::make_shared<DataTypeFloat32>()); });
//     factory.registerSimpleDataType("Float64", [] { return DataTypePtr(std::make_shared<DataTypeFloat64>()); });

//     /// These synonyms are added for compatibility.

//     factory.registerAlias("TINYINT", "Int8", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("SMALLINT", "Int16", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("INT", "Int32", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("INTEGER", "Int32", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("BIGINT", "Int64", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("FLOAT", "Float32", DataTypeFactory::CaseInsensitive);
//     factory.registerAlias("DOUBLE", "Float64", DataTypeFactory::CaseInsensitive);
// }

} // namespace doris::vectorized
