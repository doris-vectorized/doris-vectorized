#include <stdio.h>
#include "memory_pool.h"

using namespace doris;

int main()
{
    MemoryPool pool;
    pool.init();

    void* p1 = pool.alloc(sizeof(int));
    void* p2 = pool.alloc(1024);
    void* p3 = pool.alloc(1024);
    void* p4 = pool.alloc(1024);
    void* p5 = pool.alloc(1000);
    void* p6 = pool.alloc(1000);
    void* p7 = pool.alloc(10);
    void* p8 = pool.alloc(1000);
    void* p9 = pool.alloc(1000);
    void* p10 = pool.alloc(1000);
    void* p11 = pool.alloc(10*1024);
    void* p12 = pool.calloc(24);

    pool.free_large(p11);

    pool.reset();

    p12 = pool.calloc(1024);

    pool.release();

    return 0;
}

