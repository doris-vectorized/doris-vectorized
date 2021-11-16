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

#include "exec/olap_scan_node.h"

namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;

namespace vectorized {

class VOlapScanNode : public OlapScanNode {
    friend class VOlapScanner;
public:
    VOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

private:
    void scanner_thread(VOlapScanner* scanner);
    void transfer_thread(RuntimeState* state) override;
    Status start_scan_thread(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status add_one_block(Block* block);
    bool memory_consume_too_large(RuntimeState* state);
    int start_scanner_thread_task(RuntimeState* state);
    Block* get_scan_block(int assigned_thread_num);
    Block* get_block();

    std::list<Block*> _scan_blocks;
    std::mutex _scan_blocks_lock;
    std::condition_variable _scan_block_added_cv;

    std::vector<Block*> _materialized_blocks;
    std::mutex _blocks_lock;
    std::condition_variable _block_added_cv;
    std::condition_variable _block_consumed_cv;

    std::vector<Block*> _free_blocks;
    std::mutex _free_blocks_lock;

    std::list<VOlapScanner*> _volap_scanners;
    std::mutex _volap_scanners_lock;

    int _max_materialized_blocks;
    int _max_thread = 0;
};
} // namespace vectorized
} // namespace doris
