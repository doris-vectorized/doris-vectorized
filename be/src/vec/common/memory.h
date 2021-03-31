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

#include <new>

#include "common/compiler_util.h"

#if __has_include(<vec/common/config_common.h>)
#include "vec/common/config_common.h"
#endif

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>

#if JEMALLOC_VERSION_MAJOR < 4
#undef USE_JEMALLOC
#define USE_JEMALLOC 0
#include <cstdlib>
#endif
#else
#include <cstdlib>
#endif

// Also defined in Core/Defines.h
#if !defined(ALWAYS_INLINE)
#if defined(_MSC_VER)
#define ALWAYS_INLINE inline __forceinline
#else
#define ALWAYS_INLINE inline __attribute__((__always_inline__))
#endif
#endif

#if !defined(NO_INLINE)
#if defined(_MSC_VER)
#define NO_INLINE static __declspec(noinline)
#else
#define NO_INLINE __attribute__((__noinline__))
#endif
#endif

namespace Memory {

ALWAYS_INLINE void* newImpl(std::size_t size) {
    auto* ptr = malloc(size);
    if (LIKELY(ptr != nullptr)) return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc {};
}

ALWAYS_INLINE void* newNoExept(std::size_t size) noexcept {
    return malloc(size);
}

ALWAYS_INLINE void deleteImpl(void* ptr) noexcept {
    free(ptr);
}

#if USE_JEMALLOC

ALWAYS_INLINE void deleteSized(void* ptr, std::size_t size) noexcept {
    if (UNLIKELY(ptr == nullptr)) return;

    sdallocx(ptr, size, 0);
}

#else

ALWAYS_INLINE void deleteSized(void* ptr, std::size_t size [[maybe_unused]]) noexcept {
    free(ptr);
}

#endif

} // namespace Memory
