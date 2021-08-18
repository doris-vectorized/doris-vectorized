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

class MemoryPool {

    typedef unsigned char byte_t;

    enum { 
        DEFAULT_ALIGNMENT = 16,
        DEFAULT_POOL_SIZE = (16 * 1024),
    };

    struct DataBlock {
        DataBlock *next;                    //指向下一个DataBlock
        byte_t *pos;                        //下一次分配的位置
        byte_t *end;                        //结束位置
        uint32_t failed;                    //分配失败计次
    };

    struct LargeChunk {
        LargeChunk *next;
        void *p;                            //pointer to the large chunk
    };

    DataBlock *_head = nullptr;             //头结点指针
    DataBlock *_current = nullptr;          //从本current开始尝试分配内存
    struct LargeChunk *_large = nullptr;    //pointer to the head of large chunk linked list
    size_t _max = 0;                        //threshold value
    size_t _block_size = 0;                 //单个block大小
    size_t _total_allocated_bytes = 0;      //总分配字节数

public:
    bool init(size_t size = DEFAULT_POOL_SIZE) {
        assert(size > sizeof(DataBlock));
        byte_t *p = (byte_t*)aligned_alloc(DEFAULT_ALIGNMENT, size);
        if (p == nullptr) {
            return false;
        }

        _block_size = size;

        _head = _current = (DataBlock*)p;
        _head->pos = p + sizeof(DataBlock);
        _head->end = p + _block_size;
        _head->next = nullptr;
        _head->failed = 0;

        size -= sizeof(DataBlock);
        _max= (size < max_alloc_from_pool() ? size : max_alloc_from_pool());
        return true;
    }

    void release() {
        for (auto l = _large; l; l = l->next) {
            free(l->p);
        }

        for (auto p = _head; p;) {
            auto x = p;
            p = p->next;
            free(x);
        }

        _head = nullptr;
        _current = nullptr;
        _large = nullptr;
        _max = _block_size = 0;
        _total_allocated_bytes = 0;
    }

    void reset() {
        for (auto l = _large; l; l = l->next) {
            free(l->p);
        }

        for (auto p = _head; p; p = p->next) {
            p->pos = (byte_t*)p + sizeof(DataBlock);
            p->failed = 0;
        }

        _current = _head;
        _large = nullptr;
        _total_allocated_bytes = 0;
    }

    void *alloc(size_t size) {
        if (size <= _max) {
            return alloc_small(size, true);
        }
        return alloc_large(size);
    }

    void *alloc_noalign(size_t size) {
        if (size <= _max) {
            return alloc_small(size, false);
        }
        return alloc_large(size);
    }

    void *calloc(size_t size) {
        void *p = this->alloc(size);
        memset(p, 0, size);
        return p;
    }

    bool free_large(void *p) {
        for (auto l = _large; l; l = l->next) {
            if (l->p == p) {
                //printf("free large %p\n", p);
                //不减少_total_allocated_bytes，该值仅用于调试
                free(p);
                l->p = nullptr;
                return true;
            }
        }
        return false;
    }

    size_t total_allocated_bytes() const {
        return _total_allocated_bytes;
    }

private:
    static void *align_ptr(void* p, int a)
    {
        return (void*) (((uintptr_t)(p) + ((uintptr_t)a - 1)) & ~((uintptr_t)a - 1));
    }

    static size_t max_alloc_from_pool()
    {
        return getpagesize() - 1;
    }

    void *alloc_small(size_t size, bool align) {
        //printf("alloc small %u\n", (uint32_t)size);
        DataBlock *c = _current;
        byte_t *m;

        do {
            if (align) {
                m = (byte_t*)align_ptr(c->pos, DEFAULT_ALIGNMENT);
            } else {
                m = c->pos;
            }

            if (m + size <= c->end) {
                c->pos = m + size;
                _total_allocated_bytes += size;
                return m;
            }
            c = c->next;
        } while (c);

        //分配新的block
        return alloc_block(size);
    }

     void *alloc_block(size_t size) {
        //printf("alloc block %u\n", (uint32_t)size);
        byte_t *p = (byte_t*)aligned_alloc(DEFAULT_ALIGNMENT, _block_size);
        if (p == nullptr) {
            return nullptr;
        }

        byte_t *m = p + sizeof(DataBlock);
        m = (byte_t*)align_ptr(m, DEFAULT_ALIGNMENT);

        DataBlock *d = (DataBlock*)p;
        d->pos = m + size;
        d->end = p + _block_size;
        d->next = NULL;
        d->failed = 0;

        //移动_current
        auto c = _current;
        for (; c->next; c = c->next) {
            if (c->failed++ > 4) {
                _current = c->next;
            }
        }

        c->next = d;
        _total_allocated_bytes += size;
        return m;

     }

    void *alloc_large(size_t size) {
        //void *p = aligned_alloc(DEFAULT_ALIGNMENT, size);
        void *p = malloc(size);
        //printf("alloc large %u %p\n", (uint32_t)size, p);
        if (p == nullptr) {
            return nullptr;
        }

        int n = 0;
        for (auto l = _large; l; l = l->next) {
            if (l->p == nullptr) {
                l->p = p;
                _total_allocated_bytes += size;
                return p;
            }

            if (n++ > 3) {
                break;
            }
        }

        LargeChunk *l = (LargeChunk*)alloc_small(sizeof(*l), true);
        if (l == nullptr) {
            free(p);
            return nullptr;
        }
    
        l->p = p;
        l->next = _large;
        _large = l;
        _total_allocated_bytes += size;
        return p;
    }
};

}
