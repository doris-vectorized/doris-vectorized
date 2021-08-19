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

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>

namespace doris {

class MemTracker;

class MemPool {

    typedef unsigned char byte_t;

    struct alignas(64) DataBlock {
        DataBlock *next;                    //the link field
        byte_t *pos;                        //pointer to the memory for next alloc
        byte_t *end;                        //the end of this DataBlock
        uint32_t failed;                    //the alloc failed count of this DataBlock

        size_t reserved_size() const {
            return (this->end - (byte_t*)this) - sizeof(DataBlock);
        }

        size_t allocated_size() const {
            return (this->pos - (byte_t*)this) - sizeof(DataBlock);
        }
    };

    DataBlock *_head = nullptr;             //pointer to the first DataBlock
    DataBlock *_current = nullptr;          //alloc start from this DataBlock

    size_t _block_size = 0;                 //the size of each DataBlock
    size_t _block_num = 0;                  //the number of DataBlock

    size_t _total_allocated_bytes = 0;
    size_t _total_reserved_bytes = 0;
    size_t _peak_allocated_bytes = 0;

    MemTracker *_mem_tracker = nullptr;

    MemPool(const MemPool&) = delete;
    MemPool(MemPool&&) = delete;
    MemPool& operator=(const MemPool&) = delete;
    MemPool& operator=(MemPool&&) = delete;

public:
    enum { 
        DEFAULT_ALIGNMENT = 8,
        BLOCK_ALIGNMENT = 64,
        DEFAULT_POOL_SIZE = (64 * 1024),
    };

    MemPool(MemTracker* mt) : _mem_tracker(mt) {
        init();
    }

    ~MemPool() {
        release();
    }

    std::string debug_string() const {
        std::stringstream out;
        char str[16];
        out << "MemPool(#chunks=" << _block_num << " [";

        auto p = _head;
        bool first = true;
        while (p) {
            sprintf(str, "%p ", p);
            out << (first ? "" : " ") << str << p->reserved_size() 
                << "/" << p->allocated_size();
            first = false;
            p = p->next;
        }

        sprintf(str, "%p", _current);
        out << "] current=" << str
            << " total_reserved_bytes=" << _total_reserved_bytes
            << " total_allocated_bytes=" << _total_allocated_bytes << ")";

        return out.str();
    }

    bool init(size_t size = DEFAULT_POOL_SIZE) {
        if (_head) {
            return false;
        }

        assert(size > sizeof(DataBlock));
        byte_t *p = (byte_t*)aligned_alloc(BLOCK_ALIGNMENT, size);
        if (p == nullptr) {
            return false;
        }

        _block_size = size;
        _block_num = 1;

        _head = _current = (DataBlock*)p;
        _head->pos = p + sizeof(DataBlock);
        _head->end = p + _block_size;
        _head->next = nullptr;
        _head->failed = 0;

        _total_allocated_bytes = 0;
        _total_reserved_bytes = size - sizeof(DataBlock);
        _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);

        return true;
    }

    void release() {
        for (auto p = _head; p;) {
            auto x = p;
            p = p->next;
            free(x);
        }

        _head = nullptr;
        _current = nullptr;
        _block_size = 0;
        _block_num = 0;
        _total_allocated_bytes = 0;
        _total_reserved_bytes = 0;
        _peak_allocated_bytes = 0;
    }

    void clear() {
        _total_reserved_bytes = 0;
        for (auto p = _head; p; p = p->next) {
            p->pos = (byte_t*)p + sizeof(DataBlock);
            p->failed = 0;
            _total_reserved_bytes += (p->end - p->pos);
        }

        _current = _head;
        _total_allocated_bytes = 0;
        //_peak_allocated_bytes = 0;
    }

    byte_t *allocate(size_t size, int alignment=DEFAULT_ALIGNMENT) {
        return alloc(size, alignment);
    }

    OLAPStatus allocate_safely(int64_t size, uint8_t*& ret) {
        byte_t* result = allocate(size);
        if (result == nullptr) {
            return OLAP_ERR_MALLOC_ERROR;
        }
        ret = result;
        return OLAP_SUCCESS;
    }

    byte_t *try_allocate(size_t size) { 
        return allocate(size);
    }

    byte_t *try_allocate_aligned(size_t size, int alignment) {
        return allocate(size, alignment);
    }

    byte_t *try_allocate_unaligned(size_t size) {
        return allocate(size, 1);
    }

    byte_t *calloc(size_t size) {
        byte_t *p = this->allocate(size);
        memset(p, 0, size);
        return p;
    }

    void free_all() {
        release();
    }

    bool acquire_data(MemPool* src, bool keep_current) {
        /*ignore keep_current, acquire all*/
        auto tail = _head;
        while (tail->next) {
            tail = tail->next;
        }
        tail->next = src->_head;

        //_current = src->_current;
        _block_size = std::max(_block_size, src->_block_size);
        _block_num += src->_block_num;
        _total_reserved_bytes += src->_total_reserved_bytes;
        _total_allocated_bytes += src->_total_allocated_bytes;
        _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);

        src->_head = nullptr;
        src->free_all();

        return true;
    }

    void exchange_data(MemPool* other) {
        std::swap(_head, other->_head);
        std::swap(_current, other->_current);
        std::swap(_block_size, other->_block_size);
        std::swap(_block_num, other->_block_num);
        std::swap(_total_allocated_bytes, other->_total_allocated_bytes);
        std::swap(_total_reserved_bytes, other->_total_reserved_bytes);
        std::swap(_peak_allocated_bytes, other->_peak_allocated_bytes);
    }

    MemTracker* mem_tracker() { 
        return _mem_tracker; 
    }

    int64_t total_allocated_bytes() const {
        return _total_allocated_bytes;
    }

    int64_t total_reserved_bytes() const { 
        return _total_reserved_bytes; 
    }

    int64_t peak_allocated_bytes() const { 
        return _peak_allocated_bytes; 
    }

private:
    static void *align_ptr(void* p, int a)
    {
        assert((a > 0) && ((a & (a - 1)) == 0));
        return (void*) (((uintptr_t)(p) + ((uintptr_t)a - 1)) & ~((uintptr_t)a - 1));
    }

    byte_t *alloc(size_t size, int alignment) {
        //printf("alloc %u %d\n", (uint32_t)size, alignment);
        DataBlock *c = _current;

        do {
            byte_t *pos = c->pos;
            byte_t *ret = (byte_t*)align_ptr(c->pos, alignment);

            if (ret + size <= c->end) {
                c->pos = ret + size;
                auto padding = ret - pos;
                _total_allocated_bytes += (size + padding);
                _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);
                return ret;
            }
            c = c->next;
        } while (c);

        return alloc_block(size, alignment);
    }

    size_t calc_block_size(size_t size, int alignment) const {
        void* p = (void*)(sizeof(DataBlock) + size);
        return std::max(_block_size, (size_t)align_ptr(p, alignment));
    }

    byte_t *alloc_block(size_t size, int alignment) {
        auto block_size = calc_block_size(size, alignment);
        byte_t *p = (byte_t*)aligned_alloc(BLOCK_ALIGNMENT, block_size);
        if (p == nullptr) {
            return nullptr;
        }

        byte_t *pos = p + sizeof(DataBlock);
        byte_t *ret = (byte_t*)align_ptr(pos, alignment);

        DataBlock *d = (DataBlock*)p;
        d->pos = ret + size;
        d->end = p + block_size;
        d->next = NULL;
        d->failed = 0;

        //inc failed count & moving current pointer
        auto c = _current;
        for (; c->next; c = c->next) {
            if (c->failed++ >= 2) {
                _current = c->next;
            }
        }
        c->next = d;

        auto padding = ret - pos;
        _total_allocated_bytes += (size + padding);
        _total_reserved_bytes += (block_size - sizeof(DataBlock));
        _peak_allocated_bytes = std::max(_total_allocated_bytes, _peak_allocated_bytes);
        _block_num++;

        return ret;
    }
};

}