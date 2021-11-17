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

#include "vec/exec/volap_scan_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"
#include "vec/core/block.h"
#include "vec/exec/volap_scanner.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {
VOlapScanNode::VOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : OlapScanNode(pool, tnode, descs),
          _max_materialized_blocks(config::doris_scanner_queue_size) {
    _materialized_blocks.reserve(_max_materialized_blocks);
    _free_blocks.reserve(_max_materialized_blocks);
}

bool VOlapScanNode::memory_consume_too_large(RuntimeState* state) {
    int64_t mem_limit, mem_consume;
    if (auto tracker = state->fragment_mem_tracker()) {
        mem_limit = tracker->limit();
        mem_consume = tracker->consumption();
    } else {
        mem_limit = 512 * 1024 * 1024;
        mem_consume = __sync_fetch_and_add(&_buffered_bytes, 0);
    }
    return mem_consume >= (mem_limit * 6 / 10);
}

int VOlapScanNode::start_scanner_thread_task(RuntimeState* state) {
    int assigned_thread_num = 0;
    std::list<VOlapScanner*> olap_scanners;

    // copy volap scanners from _volap_scanners to local
    {
        std::lock_guard<std::mutex> lg1(_scan_blocks_lock);
        assigned_thread_num = _running_thread;

        size_t thread_slot_num = 0;
        if (!memory_consume_too_large(state)) {
            thread_slot_num = _max_thread - assigned_thread_num;
        } else { // Memory already exceed
            if (_scan_blocks.empty()) {
                if (assigned_thread_num == 0) {
                    thread_slot_num = 1;
                }
            }
        }

        {
            std::lock_guard<std::mutex> lg2(_volap_scanners_lock);
            thread_slot_num = std::min(thread_slot_num, _volap_scanners.size());
            auto it = _volap_scanners.begin(), ite = it;
            std::advance(ite, thread_slot_num);
            olap_scanners.splice(olap_scanners.end(), _volap_scanners, it, ite);
        }

        _running_thread += thread_slot_num;
        assigned_thread_num += thread_slot_num;
    }

    // post volap scanners to thread-pool
    PriorityThreadPool* thread_pool = state->exec_env()->scan_thread_pool();
    auto iter = olap_scanners.begin();
    while (iter != olap_scanners.end()) {
        PriorityThreadPool::Task task;
        task.work_function = std::bind(&VOlapScanNode::scanner_thread, this, *iter);
        task.priority = _nice;
        (*iter)->start_wait_worker_timer();
        if (thread_pool->offer(task)) {
            olap_scanners.erase(iter++);
        } else {
            LOG(FATAL) << "Failed to assign scanner task to thread pool!";
        }
        ++_total_assign_num;
    }

    return assigned_thread_num;
}

Block* VOlapScanNode::get_scan_block(int assigned_thread_num) {
    Block* scan_block = NULL;

    // scanner_row_num = 16k
    // 16k * 10 * 12 * 8 = 15M(>2s)  --> nice=10
    // 16k * 20 * 22 * 8 = 55M(>6s)  --> nice=0
    while (_nice > 0 && _total_assign_num > (22 - _nice) * (20 - _nice) * 6) {
        --_nice;
    }

    {
        // 1 scanner idle task not empty, assign new scanner task
        std::unique_lock<std::mutex> l(_scan_blocks_lock);

        // 2 wait when all scanner are running & no result in queue
        while (UNLIKELY(_running_thread == assigned_thread_num && _scan_blocks.empty() &&
                        !_scanner_done)) {
            SCOPED_TIMER(_scanner_wait_batch_timer);
            _scan_block_added_cv.wait(l);
        }

        // 3 transfer result block when queue is not empty
        if (LIKELY(!_scan_blocks.empty())) {
            scan_block = _scan_blocks.front();
            _scan_blocks.pop_front();

            // delete scan_block if transfer thread should be stopped
            // because scan_block wouldn't be useful anymore
            if (UNLIKELY(_transfer_done)) {
                delete scan_block;
                scan_block = NULL;
            }
        }
    }

    return scan_block;
}

void VOlapScanNode::transfer_thread(RuntimeState* state) {
    // scanner open pushdown to scanThread
    Status status = Status::OK();

    if (_vconjunct_ctx_ptr) {
        for (auto scanner : _volap_scanners) {
            status = (*_vconjunct_ctx_ptr)->clone(state, scanner->vconjunct_ctx_ptr());
            if (!status.ok()) {
                std::lock_guard<SpinLock> guard(_status_mutex);
                _status = status;
                break;
            }
        }
    }

    /*********************************
     * 优先级调度基本策略:
     * 1. 通过查询拆分的Range个数来确定初始nice值
     *    Range个数越多，越倾向于认定为大查询，nice值越小
     * 2. 通过查询累计读取的数据量来调整nice值
     *    读取的数据越多，越倾向于认定为大查询，nice值越小
     * 3. 通过nice值来判断查询的优先级
     *    nice值越大的，越优先获得的查询资源
     * 4. 定期提高队列内残留任务的优先级，避免大查询完全饿死
     *********************************/
    _total_assign_num = 0;
    _nice = 18 + std::max(0, 2 - (int)_volap_scanners.size() / 5);

    _max_thread = _max_materialized_blocks;
    if (config::doris_scanner_row_num > state->batch_size()) {
        _max_thread /= config::doris_scanner_row_num / state->batch_size();
    }

    // read from scanner
    while (LIKELY(status.ok())) {
        int assigned_thread_num = start_scanner_thread_task(state);
        if (Block* scan_block = get_scan_block(assigned_thread_num)) {
            add_one_block(scan_block);
        } else if (_scanner_done) {
            break;
        }
    }

    VLOG_CRITICAL << "TransferThread finish.";
    _transfer_done = true;
    _block_added_cv.notify_all();

    {
        std::unique_lock<std::mutex> l(_scan_blocks_lock);
        _scan_thread_exit_cv.wait(l, [this] { return _running_thread == 0; });
    }
    VLOG_CRITICAL << "Scanner threads have been exited. TransferThread exit.";
}

Block* VOlapScanNode::alloc_block() {
    {
        std::lock_guard<std::mutex> l(_free_blocks_lock);
        if (!_free_blocks.empty()) {
            auto block = _free_blocks.back();
            _free_blocks.pop_back();
            return block;
        }
    }
    return new Block();
}

void VOlapScanNode::scanner_thread(VOlapScanner* scanner) {
    int64_t wait_time = scanner->update_wait_worker_timer();
    // Do not use ScopedTimer. There is no guarantee that, the counter
    // (_scan_cpu_timer, the class member) is not destroyed after `_running_thread==0`.
    ThreadCpuStopWatch cpu_watch;
    cpu_watch.start();
    Status status = Status::OK();
    bool eos = false;
    RuntimeState* state = scanner->runtime_state();
    DCHECK(NULL != state);
    if (!scanner->is_open()) {
        status = scanner->open();
        if (!status.ok()) {
            std::lock_guard<SpinLock> guard(_status_mutex);
            _status = status;
            eos = true;
        }
        scanner->set_opened();
    }

    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceed threshold, we yield this thread.
    std::list<Block*> blocks;

    int64_t raw_rows_read = scanner->raw_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    while (!eos && raw_rows_read < raw_rows_threshold) {
        if (UNLIKELY(_transfer_done)) {
            eos = true;
            status = Status::Cancelled("Cancelled");
            LOG(INFO) << "Scan thread cancelled, cause query done, maybe reach limit.";
            break;
        }

        Block* block = alloc_block();
        status = scanner->get_block(_runtime_state, block, &eos);
        VLOG_ROW << "VOlapScanNode input rows: " << block->rows();
        if (!status.ok()) {
            LOG(WARNING) << "Scan thread read OlapScanner failed: " << status.to_string();
            eos = true;
            delete block;
            break;
        }
        // 4. if status not ok, change status_.
        if (UNLIKELY(block->rows() == 0)) {
            // may be failed, push already, scan node delete this block.
            delete block;
            block = NULL;
        } else {
            blocks.push_back(block);
            // TODO(yangzhg) this bytes is an approximate number
            __sync_fetch_and_add(&_buffered_bytes, block->allocated_bytes());
        }
        raw_rows_read = scanner->raw_rows_read();
    }

    {
        // if we failed, check status.
        if (UNLIKELY(!status.ok())) {
            _transfer_done = true;
            std::lock_guard<SpinLock> guard(_status_mutex);
            if (LIKELY(_status.ok())) {
                _status = status;
            }
        }  
        
        bool global_status_ok = false;
        {
            std::lock_guard<SpinLock> guard(_status_mutex);
            global_status_ok = _status.ok();
        }

        if (UNLIKELY(!global_status_ok)) {
            eos = true;
            std::for_each(blocks.begin(), blocks.end(), std::default_delete<Block>());
        } else {
            std::lock_guard<std::mutex> l(_scan_blocks_lock);
            _scan_blocks.splice(_scan_blocks.end(), blocks);
        }

        // If eos is true, we will process out of this lock block.
        if (!eos) {
            std::lock_guard<std::mutex> l(_volap_scanners_lock);
            _volap_scanners.push_front(scanner);
        }
    }

    if (eos) {
        // close out of blocks lock. we do this before _progress update
        // that can assure this object can keep live before we finish.
        scanner->close(_runtime_state);

        std::lock_guard<std::mutex> l(_scan_blocks_lock);
        _progress.update(1);
        if (_progress.done()) {
            // this is the right out
            _scanner_done = true;
        }
    }

    _scan_cpu_timer->update(cpu_watch.elapsed_time());
    _scanner_wait_worker_timer->update(wait_time);

    // The transfer thead will wait for `_running_thread==0`, to make sure all scanner threads won't access class members.
    // Do not access class members after this code.
    {
        std::lock_guard<std::mutex> l(_scan_blocks_lock);
        _running_thread--;
    }

    _scan_block_added_cv.notify_one();
    _scan_thread_exit_cv.notify_one();
}

Status VOlapScanNode::add_one_block(Block* block) {
    {
        std::unique_lock<std::mutex> l(_blocks_lock);

        while (UNLIKELY(_materialized_blocks.size() >= _max_materialized_blocks &&
                        !_transfer_done)) {
            _block_consumed_cv.wait(l);
        }

        VLOG_CRITICAL << "Push block to materialized_blocks";
        _materialized_blocks.push_back(block);
        _mem_tracker->Consume(block->allocated_bytes());
    }
    // remove one block, notify main thread
    _block_added_cv.notify_one();
    return Status::OK();
}

Status VOlapScanNode::start_scan_thread(RuntimeState* state) {
    if (_scan_ranges.empty()) {
        _transfer_done = true;
        return Status::OK();
    }

    // ranges constructed from scan keys
    std::vector<std::unique_ptr<OlapScanRange>> cond_ranges;
    RETURN_IF_ERROR(_scan_keys.get_key_range(&cond_ranges));
    // if we can't get ranges from conditions, we give it a total range
    if (cond_ranges.empty()) {
        cond_ranges.emplace_back(new OlapScanRange());
    }

    bool need_split = true;
    // If we have ranges more than 64, there is no need to call
    // ShowHint to split ranges
    if (limit() != -1 || cond_ranges.size() > 64) {
        need_split = false;
    }

    int scanners_per_tablet = std::max(1, 64 / (int)_scan_ranges.size());

    std::unordered_set<std::string> disk_set;
    for (auto& scan_range : _scan_ranges) {
        std::vector<std::unique_ptr<OlapScanRange>>* ranges = &cond_ranges;
        std::vector<std::unique_ptr<OlapScanRange>> split_ranges;
        if (need_split) {
            auto st = OlapScanNode::get_hints(*scan_range, config::doris_scan_range_row_count,
                                              _scan_keys.begin_include(), _scan_keys.end_include(),
                                              cond_ranges, &split_ranges, _runtime_profile.get());
            if (st.ok()) {
                ranges = &split_ranges;
            }
        }

        int ranges_per_scanner = std::max(1, (int)ranges->size() / scanners_per_tablet);
        int num_ranges = ranges->size();
        for (int i = 0; i < num_ranges;) {
            std::vector<OlapScanRange*> scanner_ranges;
            scanner_ranges.push_back((*ranges)[i].get());
            ++i;
            for (int j = 1; i < num_ranges && j < ranges_per_scanner &&
                            (*ranges)[i]->end_include == (*ranges)[i - 1]->end_include;
                 ++j, ++i) {
                scanner_ranges.push_back((*ranges)[i].get());
            }
            VOlapScanner* scanner =
                    new VOlapScanner(state, this, _olap_scan_node.is_preaggregation,
                                     _need_agg_finalize, *scan_range);
            // add scanner to pool before doing prepare.
            // so that scanner can be automatically deconstructed if prepare failed.
            _scanner_pool.add(scanner);
            RETURN_IF_ERROR(scanner->prepare(*scan_range, scanner_ranges, _olap_filter, _bloom_filters_push_down));

            _volap_scanners.push_back(scanner);
            disk_set.insert(scanner->scan_disk());
        }
    }
    COUNTER_SET(_num_disks_accessed_counter, static_cast<int64_t>(disk_set.size()));
    COUNTER_SET(_num_scanners, static_cast<int64_t>(_volap_scanners.size()));

    // init progress
    std::stringstream ss;
    ss << "ScanThread complete (node=" << id() << "):";
    _progress = ProgressUpdater(ss.str(), _volap_scanners.size(), 1);

    _transfer_thread.reset(new std::thread(&VOlapScanNode::transfer_thread, this, state));

    return Status::OK();
}

Status VOlapScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    // change done status
    _transfer_done = true;

    // notify all scanner thread
    _block_consumed_cv.notify_all();
    _block_added_cv.notify_all();
    _scan_block_added_cv.notify_all();

    // join transfer thread
    _transfer_thread->join();

    size_t mem_usege_in_block = 0;
    // clear some block in queue
    for (auto block : _materialized_blocks) {
        mem_usege_in_block += block->allocated_bytes();
        delete block;
    }
    _materialized_blocks.clear();
    _mem_tracker->Release(mem_usege_in_block);

    for (auto block : _scan_blocks) {
        delete block;
    }
    _scan_blocks.clear();

    for (auto block : _free_blocks) {
        delete block;
    }

    // OlapScanNode terminate by exception
    // so that initiative close the Scanner
    for (auto scanner : _volap_scanners) {
        scanner->close(state);
    }

    VLOG_CRITICAL << "VOlapScanNode::close()";
    return ScanNode::close(state);
}

Status VOlapScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    // check if Canceled.
    if (state->is_cancelled()) {
        _transfer_done = true;
        std::lock_guard<SpinLock> guard(_status_mutex);
        if (LIKELY(_status.ok())) {
            _status = Status::Cancelled("Cancelled");
        }
        return _status;
    }

    // check if started.
    if (!_start) {
        Status status = start_scan(state);

        if (!status.ok()) {
            LOG(ERROR) << "StartScan Failed cause " << status.get_error_msg();
            *eos = true;
            return status;
        }

        _start = true;
    }

    // some conjuncts will be disposed in start_scan function, so
    // we should check _eos after call start_scan
    if (_eos) {
        *eos = true;
        return Status::OK();
    }

    // wait for block from queue
    Block* materialized_block = NULL;
    {
        std::unique_lock<std::mutex> l(_blocks_lock);
        SCOPED_TIMER(_olap_wait_batch_queue_timer);
        while (_materialized_blocks.empty() && !_transfer_done) {
            if (state->is_cancelled()) {
                _transfer_done = true;
            }

            // use wait_for, not wait, in case to capture the state->is_cancelled()
            _block_added_cv.wait_for(l, std::chrono::seconds(1));
        }

        if (!_materialized_blocks.empty()) {
            materialized_block = _materialized_blocks.back();
            DCHECK(materialized_block != NULL);
            _materialized_blocks.pop_back();
        }
    }

    // return block
    if (NULL != materialized_block) {
        // notify scanner
        _block_consumed_cv.notify_one();
        // get scanner's block memory
        block->swap(*materialized_block);
        VLOG_ROW << "VOlapScanNode output rows: " << block->rows();
        _num_rows_returned += block->rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        _mem_tracker->Release(block->allocated_bytes());

        // reach scan node limit
        if (reached_limit()) {
            int num_rows_over = _num_rows_returned - _limit;
            block->set_num_rows(block->rows() - num_rows_over);

            _num_rows_returned -= num_rows_over;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            _transfer_done = true;

            _block_consumed_cv.notify_all();
            *eos = true;
            LOG(INFO) << "VOlapScanNode ReachedLimit.";
        } else {
            *eos = false;
        }

        // if (VLOG_ROW_IS_ON) {
        //     for (int i = 0; i < block->rows(); ++i) {
        //         TupleRow* row = block->get_row(i);
        //         VLOG_ROW << "VOlapScanNode output row: "
        //                  << Tuple::to_string(row->get_tuple(0), *_tuple_desc);
        //     }
        // }
        __sync_fetch_and_sub(&_buffered_bytes, block->allocated_bytes());

        {
            // ReThink whether the SpinLock Better
            std::lock_guard<std::mutex> l(_free_blocks_lock);
            _free_blocks.emplace_back(materialized_block);
        }
        return Status::OK();
    }

    // all scanner done, change *eos to true
    *eos = true;
    std::lock_guard<SpinLock> guard(_status_mutex);
    return _status;
}

} // namespace doris::vectorized
