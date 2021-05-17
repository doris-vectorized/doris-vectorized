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

// more aliases: https://mailman.videolan.org/pipermail/x264-devel/2014-May/010660.html
#ifdef ALWAYS_INLINE
#undef ALWAYS_INLINE
#endif
#define ALWAYS_INLINE __attribute__((__always_inline__))
#define NO_INLINE __attribute__((__noinline__))
#define MAY_ALIAS __attribute__((__may_alias__))

/// Check for presence of address sanitizer
#if !defined(ADDRESS_SANITIZER)
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define ADDRESS_SANITIZER 1
#endif
#elif defined(__SANITIZE_ADDRESS__)
#define ADDRESS_SANITIZER 1
#endif
#endif

#if !defined(THREAD_SANITIZER)
#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define THREAD_SANITIZER 1
#endif
#elif defined(__SANITIZE_THREAD__)
#define THREAD_SANITIZER 1
#endif
#endif

#if !defined(MEMORY_SANITIZER)
#if defined(__has_feature)
#if __has_feature(memory_sanitizer)
#define MEMORY_SANITIZER 1
#endif
#elif defined(__MEMORY_SANITIZER__)
#define MEMORY_SANITIZER 1
#endif
#endif

/// TODO Strange enough, there is no way to detect UB sanitizer.

/// Explicitly allow undefined behaviour for certain functions. Use it as a function attribute.
/// It is useful in case when compiler cannot see (and exploit) it, but UBSan can.
/// Example: multiplication of signed integers with possibility of overflow when both sides are from user input.
#if defined(__clang__)
#define NO_SANITIZE_UNDEFINED __attribute__((__no_sanitize__("undefined")))
#define NO_SANITIZE_ADDRESS __attribute__((__no_sanitize__("address")))
#define NO_SANITIZE_THREAD __attribute__((__no_sanitize__("thread")))
#else
/// It does not work in GCC. GCC 7 cannot recognize this attribute and GCC 8 simply ignores it.
#define NO_SANITIZE_UNDEFINED
#define NO_SANITIZE_ADDRESS
#define NO_SANITIZE_THREAD
#endif
