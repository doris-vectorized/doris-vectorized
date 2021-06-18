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

#include <snappy/snappy.h>

#include <iostream>

#include "gen_cpp/data.pb.h"
#include "util/string_parser.hpp"
#include "vec/common/arena.h"
#include "vec/common/exception.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/io/reader_buffer.h"
#include "vec/io/var_int.h"
#include "util/binary_cast.hpp"

#define DEFAULT_MAX_STRING_SIZE (1ULL << 30)
#define WRITE_HELPERS_MAX_INT_WIDTH 40U

namespace doris::vectorized {

template <typename T>
inline T decimalScaleMultiplier(UInt32 scale);
template <>
inline Int32 decimalScaleMultiplier<Int32>(UInt32 scale) {
    return common::exp10_i32(scale);
}
template <>
inline Int64 decimalScaleMultiplier<Int64>(UInt32 scale) {
    return common::exp10_i64(scale);
}
template <>
inline Int128 decimalScaleMultiplier<Int128>(UInt32 scale) {
    return common::exp10_i128(scale);
}

inline std::string int128_to_string(__int128_t value) {
    char buffer[128];
    char* d = std::end(buffer);
    do {
        --d;
        *d = "0123456789"[value % 10];
        value /= 10;
    } while (value != 0);
    if (value < 0) {
        --d;
        *d = '-';
    }
    int len = std::end(buffer) - d;
    return std::string(d, len);
}

inline std::string int128_to_string(UInt128 value) {
    return value.toHexString();
}

template <typename T>
void writeText(Decimal<T> value, UInt32 scale, std::ostream& ostr) {
    if (value < Decimal<T>(0)) {
        value *= Decimal<T>(-1);
        ostr << '-';
    }

    T whole_part = value;
    if (scale) {
        whole_part = value / decimalScaleMultiplier<T>(scale);
    }
    if constexpr (std::is_same<T, __int128_t>::value || std::is_same<T, UInt128>::value) {
        ostr << int128_to_string(whole_part);
    } else {
        ostr << whole_part;
    }
    if (scale) {
        ostr << '.';
        String str_fractional(scale, '0');
        for (Int32 pos = scale - 1; pos >= 0; --pos, value /= Decimal<T>(10))
            str_fractional[pos] += value % Decimal<T>(10);
        ostr.write(str_fractional.data(), scale);
    }
}
/// Methods for output in binary format.

/// Write POD-type in native format. It's recommended to use only with packed (dense) data types.
template <typename Type>
inline void writePODBinary(const Type& x, std::ostream& buf) {
    buf.write(reinterpret_cast<const char*>(&x), sizeof(x));
}

template <typename Type>
inline void writeIntBinary(const Type& x, std::ostream& buf) {
    writePODBinary(x, buf);
}

template <typename Type>
inline void writeFloatBinary(const Type& x, std::ostream& buf) {
    writePODBinary(x, buf);
}

inline void writeStringBinary(const std::string& s, std::ostream& buf) {
    writeVarUInt(s.size(), buf);
    buf.write(s.data(), s.size());
}

inline void writeStringBinary(const StringRef& s, std::ostream& buf) {
    writeVarUInt(s.size, buf);
    buf.write(s.data, s.size);
}

inline void writeStringBinary(const char* s, std::ostream& buf) {
    writeStringBinary(StringRef{s}, buf);
}

template <typename Type>
void writeVectorBinary(const std::vector<Type>& v, std::ostream& buf) {
    writeVarUInt(v.size(), buf);

    for (typename std::vector<Type>::const_iterator it = v.begin(); it != v.end(); ++it)
        writeBinary(*it, buf);
}

template <typename Type>
inline std::enable_if_t<std::is_arithmetic_v<Type>, void> writeBinary(const Type& x,
                                                                      std::ostream& buf) {
    writePODBinary(x, buf);
}

inline void writeBinary(const String& x, std::ostream& buf) {
    writeStringBinary(x, buf);
}
inline void writeBinary(const StringRef& x, std::ostream& buf) {
    writeStringBinary(x, buf);
}
inline void writeBinary(const Int128& x, std::ostream& buf) {
    writePODBinary(x, buf);
}
inline void writeBinary(const UInt128& x, std::ostream& buf) {
    writePODBinary(x, buf);
}

inline void writeBinary(const Decimal32& x, std::ostream& buf) {
    writePODBinary(x, buf);
}
inline void writeBinary(const Decimal64& x, std::ostream& buf) {
    writePODBinary(x, buf);
}
inline void writeBinary(const Decimal128& x, std::ostream& buf) {
    writePODBinary(x, buf);
}
inline void write_binary(const std::ostringstream& buf, PColumn* pcolumn) {
    std::string uncompressed = buf.str();
    std::string compressed;
    snappy::Compress(uncompressed.data(), uncompressed.size(), &compressed);
    if (static_cast<double>(compressed.size()) / uncompressed.size() > 0.7) {
        pcolumn->set_compressed(false);
        pcolumn->mutable_binary()->append(uncompressed);
    } else {
        pcolumn->set_compressed(true);
        pcolumn->mutable_binary()->append(compressed);
    }
}

/// Read POD-type in native format
template <typename Type>
inline void readPODBinary(Type& x, std::istream& buf) {
    buf.read(reinterpret_cast<char*>(&x), sizeof(x));
    if (!buf) {
        throw Poco::Exception("Cannot read all data. Bytes read: " + std::to_string(buf.gcount()) +
                              ". Bytes expected: " + std::to_string(sizeof(x)) + ".");
    }
}

template <typename Type>
inline void readIntBinary(Type& x, std::istream& buf) {
    readPODBinary(x, buf);
}

template <typename Type>
inline void readFloatBinary(Type& x, std::istream& buf) {
    readPODBinary(x, buf);
}

inline void readStringBinary(std::string& s, std::istream& buf,
                             size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE) {
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_STRING_SIZE) throw Poco::Exception("Too large string size.");

    s.resize(size);
    buf.read(s.data(), size);
    if (!buf) {
        throw Poco::Exception("Cannot read all data. Bytes read: " + std::to_string(buf.gcount()) +
                              ". Bytes expected: " + std::to_string(size) + ".");
    }
}

inline StringRef readStringBinaryInto(Arena& arena, std::istream& buf) {
    size_t size = 0;
    readVarUInt(size, buf);

    char* data = arena.alloc(size);
    buf.read(data, size);

    return StringRef(data, size);
}

template <typename Type>
void readVectorBinary(std::vector<Type>& v, std::istream& buf,
                      size_t MAX_VECTOR_SIZE = DEFAULT_MAX_STRING_SIZE) {
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_VECTOR_SIZE) throw Poco::Exception("Too large vector size.");

    v.resize(size);
    for (size_t i = 0; i < size; ++i) readBinary(v[i], buf);
}

/// Generic methods to read value in native binary format.
template <typename Type>
inline std::enable_if_t<std::is_arithmetic_v<Type>, void> readBinary(Type& x, std::istream& buf) {
    readPODBinary(x, buf);
}

inline void readBinary(String& x, std::istream& buf) {
    readStringBinary(x, buf);
}
inline void readBinary(Int128& x, std::istream& buf) {
    readPODBinary(x, buf);
}
inline void readBinary(UInt128& x, std::istream& buf) {
    readPODBinary(x, buf);
}

inline void readBinary(Decimal32& x, std::istream& buf) {
    readPODBinary(x, buf);
}
inline void readBinary(Decimal64& x, std::istream& buf) {
    readPODBinary(x, buf);
}
inline void readBinary(Decimal128& x, std::istream& buf) {
    readPODBinary(x, buf);
}
inline void read_binary(const PColumn& pcolumn, std::string* data) {
    if (pcolumn.compressed()) {
        snappy::Uncompress(pcolumn.binary().data(), pcolumn.binary().size(), data);
    } else {
        *data = pcolumn.binary();
    }
}

template <typename T>
bool readFloatTextFastImpl(T & x, ReadBuffer & in) {
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII"); //-V590

    StringParser::ParseResult result;
    x = StringParser::string_to_float<T>(in.position(), in.count(), &result);

    if (UNLIKELY(result != StringParser::PARSE_SUCCESS || std::isnan(x) || std::isinf(x))) {                                                    \
        return false;
    }

    // only to match the isAllRead() check to prevent return null
    in.position() = in.end();
    return true;
}

template <typename T>
bool readIntTextImpl(T & x, ReadBuffer & buf) {
    bool negative = false;
    std::make_unsigned_t<T> res = 0;
    if (buf.eof()) {
        return false;
    }

    while (!buf.eof())
    {
        switch (*buf.position())
        {
            case '+':
                break;
            case '-':
                if (std::is_signed_v<T>)
                    negative = true;
                else
                {
                    return false;
                }
                break;
            case '0': [[fallthrough]];
            case '1': [[fallthrough]];
            case '2': [[fallthrough]];
            case '3': [[fallthrough]];
            case '4': [[fallthrough]];
            case '5': [[fallthrough]];
            case '6': [[fallthrough]];
            case '7': [[fallthrough]];
            case '8': [[fallthrough]];
            case '9':
                res *= 10;
                res += *buf.position() - '0';
                break;
            default:
                x = negative ? -res : res;
                return true;
        }
        ++buf.position();
    }

    x = negative ? -res : res;
    return true;
}

template <typename T>
bool readDateTimeTextImpl(T& x, ReadBuffer & buf) {
    static_assert(std::is_same_v<Int128, T>);
    auto dv = binary_cast<Int128, DateTimeValue>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count());

    // only to match the isAllRead() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<DateTimeValue, Int128>(x);
    return ans;
}

template <typename T>
bool readDecimalTextImpl(T& x, ReadBuffer & buf) {
    static_assert(IsDecimalNumber<T>);
    // TODO: open this static_assert
    // static_assert(std::is_same_v<Decimal128, T>);
    auto dv = binary_cast<Int128, DecimalV2Value>(x.value);
    auto ans = dv.parse_from_str((const char *) buf.position(), buf.count()) == 0;

    // only to match the isAllRead() check to prevent return null
    buf.position() = buf.end();

    x.value = binary_cast<DecimalV2Value, Int128>(dv);
    return ans;
}

template <typename T>
bool tryReadIntText(T & x, ReadBuffer & buf) {
    return readIntTextImpl<T>(x, buf);
}

template <typename T>
bool tryReadFloatText(T & x, ReadBuffer & in) {
    return readFloatTextFastImpl<T>(x, in);
}

template <typename T>
bool tryReadDecimalText(T & x, ReadBuffer & in) {
    return readDecimalTextImpl<T>(x, in);
}

template <typename T>
bool tryReadDateTimeText(T & x, ReadBuffer & in) {
    return readDateTimeTextImpl<T>(x, in);
}
} // namespace doris::vectorized
