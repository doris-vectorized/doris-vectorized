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

#include <memory>

#include "exec/mysql_scan_node.h"
#include "exec/mysql_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"
namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RowBatch;
class TextConverter;
class TupleDescriptor;
class RuntimeState;
class MemPool;
class Status;
namespace vectorized {

class VMysqlScanNode : public ExecNode {
public:
    VMysqlScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~VMysqlScanNode();

    //initialize _mysql_scanner, and create _text_converter.
    virtual Status prepare(RuntimeState* state);

    // Start MySQL scan using _mysql_scanner.
    virtual Status open(RuntimeState* state);

    // Fill the next row batch by calling next() on the _mysql_scanner,
    // converting text data in MySQL cells to binary data.
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos);

    // Close the _mysql_scanner, and report errors.
    virtual Status close(RuntimeState* state);

    virtual void debug_string(int indentation_level, std::stringstream* out) const;

    Status write_text_slot(char* value, int value_length, SlotDescriptor* slot,
                           vectorized::MutableColumnPtr* column_ptr, RuntimeState* state);

private:
    bool _is_init;
    MysqlScannerParam _my_param;
    std::string _table_name;
    TupleId _tuple_id;
    std::vector<std::string> _columns;
    std::vector<std::string> _filters;
    const TupleDescriptor* _tuple_desc;
    int _slot_num;
    std::unique_ptr<MemPool> _tuple_pool;
    std::unique_ptr<MysqlScanner> _mysql_scanner;
    std::unique_ptr<TextConverter> _text_converter;
};
} // namespace vectorized
} // namespace doris