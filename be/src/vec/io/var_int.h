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

#include <iostream>

#include "vec/core/types.h"

namespace doris::vectorized {
namespace ErrorCodes {
extern const int ATTEMPT_TO_READ_AFTER_EOF;
}

/** Write UInt64 in variable length format (base128) NOTE Only up to 2^63 - 1 are supported. */
void writeVarUInt(UInt64 x, std::ostream& ostr);
char* writeVarUInt(UInt64 x, char* ostr);

/** Read UInt64, written in variable length format (base128) */
void readVarUInt(UInt64& x, std::istream& istr);
const char* readVarUInt(UInt64& x, const char* istr, size_t size);

/** Get the length of UInt64 in VarUInt format */
size_t getLengthOfVarUInt(UInt64 x);

/** Get the Int64 length in VarInt format */
size_t getLengthOfVarInt(Int64 x);

/** Write Int64 in variable length format (base128) */
template <typename OUT>
inline void writeVarInt(Int64 x, OUT& ostr) {
    writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char* writeVarInt(Int64 x, char* ostr) {
    return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

/** Read Int64, written in variable length format (base128) */
template <typename IN>
inline void readVarInt(Int64& x, IN& istr) {
    readVarUInt(*reinterpret_cast<UInt64*>(&x), istr);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

inline const char* readVarInt(Int64& x, const char* istr, size_t size) {
    const char* res = readVarUInt(*reinterpret_cast<UInt64*>(&x), istr, size);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
    return res;
}

inline void writeVarT(UInt64 x, std::ostream& ostr) {
    writeVarUInt(x, ostr);
}
inline void writeVarT(Int64 x, std::ostream& ostr) {
    writeVarInt(x, ostr);
}

inline char* writeVarT(UInt64 x, char*& ostr) {
    return writeVarUInt(x, ostr);
}
inline char* writeVarT(Int64 x, char*& ostr) {
    return writeVarInt(x, ostr);
}

inline void readVarT(UInt64& x, std::istream& istr) {
    readVarUInt(x, istr);
}
inline void readVarT(Int64& x, std::istream& istr) {
    readVarInt(x, istr);
}

inline const char* readVarT(UInt64& x, const char* istr, size_t size) {
    return readVarUInt(x, istr, size);
}
inline const char* readVarT(Int64& x, const char* istr, size_t size) {
    return readVarInt(x, istr, size);
}

inline void readVarUInt(UInt64& x, std::istream& istr) {
    x = 0;
    for (size_t i = 0; i < 9; ++i) {
        UInt64 byte = istr.get();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80)) return;
    }
}

inline void writeVarUInt(UInt64 x, std::ostream& ostr) {
    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) byte |= 0x80;

        ostr.put(byte);

        x >>= 7;
        if (!x) return;
    }
}

inline char* writeVarUInt(UInt64 x, char* ostr) {
    for (size_t i = 0; i < 9; ++i) {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F) byte |= 0x80;

        *ostr = byte;
        ++ostr;

        x >>= 7;
        if (!x) return ostr;
    }

    return ostr;
}

inline size_t getLengthOfVarUInt(UInt64 x) {
    return x < (1ULL << 7)
                   ? 1
                   : (x < (1ULL << 14)
                              ? 2
                              : (x < (1ULL << 21)
                                         ? 3
                                         : (x < (1ULL << 28)
                                                    ? 4
                                                    : (x < (1ULL << 35)
                                                               ? 5
                                                               : (x < (1ULL << 42)
                                                                          ? 6
                                                                          : (x < (1ULL << 49)
                                                                                     ? 7
                                                                                     : (x < (1ULL
                                                                                             << 56)
                                                                                                ? 8
                                                                                                : 9)))))));
}

inline size_t getLengthOfVarInt(Int64 x) {
    return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

} // namespace doris::vectorized
