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

//#include <IO/ReadBuffer.h>
//#include <IO/WriteBuffer.h>
//#include <IO/ReadHelpers.h>
//#include <IO/WriteHelpers.h>

#include "vec/core/field.h"

#include "vec/core/decimal_comparison.h"
#include "vec/io/io_helper.h"
//#include <vec/Common/FieldVisitors.h>

namespace doris::vectorized {
//    void read_binary(Array & x, ReadBuffer & buf)
//    {
//        size_t size;
//        UInt8 type;
//        doris::vectorized::read_binary(type, buf);
//        doris::vectorized::read_binary(size, buf);
//
//        for (size_t index = 0; index < size; ++index)
//        {
//            switch (type)
//            {
//                case Field::Types::Null:
//                {
//                    x.push_back(doris::vectorized::Field());
//                    break;
//                }
//                case Field::Types::UInt64:
//                {
//                    UInt64 value;
//                    doris::vectorized::read_var_uint(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::UInt128:
//                {
//                    UInt128 value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Int64:
//                {
//                    Int64 value;
//                    doris::vectorized::read_var_int(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Float64:
//                {
//                    Float64 value;
//                    doris::vectorized::read_float_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::String:
//                {
//                    std::string value;
//                    doris::vectorized::read_string_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Array:
//                {
//                    Array value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Tuple:
//                {
//                    Tuple value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::AggregateFunctionState:
//                {
//                    AggregateFunctionStateData value;
//                    doris::vectorized::read_string_binary(value.name, buf);
//                    doris::vectorized::read_string_binary(value.data, buf);
//                    x.push_back(value);
//                    break;
//                }
//            }
//        }
//    }
//
//    void write_binary(const Array & x, WriteBuffer & buf)
//    {
//        UInt8 type = Field::Types::Null;
//        size_t size = x.size();
//        if (size)
//            type = x.front().getType();
//        doris::vectorized::write_binary(type, buf);
//        doris::vectorized::write_binary(size, buf);
//
//        for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
//        {
//            switch (type)
//            {
//                case Field::Types::Null: break;
//                case Field::Types::UInt64:
//                {
//                    doris::vectorized::write_var_uint(get<UInt64>(*it), buf);
//                    break;
//                }
//                case Field::Types::UInt128:
//                {
//                    doris::vectorized::write_binary(get<UInt128>(*it), buf);
//                    break;
//                }
//                case Field::Types::Int64:
//                {
//                    doris::vectorized::write_var_int(get<Int64>(*it), buf);
//                    break;
//                }
//                case Field::Types::Float64:
//                {
//                    doris::vectorized::write_float_binary(get<Float64>(*it), buf);
//                    break;
//                }
//                case Field::Types::String:
//                {
//                    doris::vectorized::write_string_binary(get<std::string>(*it), buf);
//                    break;
//                }
//                case Field::Types::Array:
//                {
//                    doris::vectorized::write_binary(get<Array>(*it), buf);
//                    break;
//                }
//                case Field::Types::Tuple:
//                {
//                    doris::vectorized::write_binary(get<Tuple>(*it), buf);
//                    break;
//                }
//                case Field::Types::AggregateFunctionState:
//                {
//                    doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().name, buf);
//                    doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().data, buf);
//                    break;
//                }
//            }
//        }
//    }
//
//    void write_text(const Array & x, WriteBuffer & buf)
//    {
//        doris::vectorized::String res = applyVisitor(doris::vectorized::FieldVisitorToString(), doris::vectorized::Field(x));
//        buf.write(res.data(), res.size());
//    }
//
//    void read_binary(Tuple & x, ReadBuffer & buf)
//    {
//        size_t size;
//        doris::vectorized::read_binary(size, buf);
//
//        for (size_t index = 0; index < size; ++index)
//        {
//            UInt8 type;
//            doris::vectorized::read_binary(type, buf);
//
//            switch (type)
//            {
//                case Field::Types::Null:
//                {
//                    x.push_back(doris::vectorized::Field());
//                    break;
//                }
//                case Field::Types::UInt64:
//                {
//                    UInt64 value;
//                    doris::vectorized::read_var_uint(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::UInt128:
//                {
//                    UInt128 value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Int64:
//                {
//                    Int64 value;
//                    doris::vectorized::read_var_int(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Float64:
//                {
//                    Float64 value;
//                    doris::vectorized::read_float_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::String:
//                {
//                    std::string value;
//                    doris::vectorized::read_string_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Array:
//                {
//                    Array value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::Tuple:
//                {
//                    Tuple value;
//                    doris::vectorized::read_binary(value, buf);
//                    x.push_back(value);
//                    break;
//                }
//                case Field::Types::AggregateFunctionState:
//                {
//                    AggregateFunctionStateData value;
//                    doris::vectorized::read_string_binary(value.name, buf);
//                    doris::vectorized::read_string_binary(value.data, buf);
//                    x.push_back(value);
//                    break;
//                }
//            }
//        }
//    }
//
//    void write_binary(const Tuple & x, WriteBuffer & buf)
//    {
//        const size_t size = x.size();
//        doris::vectorized::write_binary(size, buf);
//
//        for (auto it = x.begin(); it != x.end(); ++it)
//        {
//            const UInt8 type = it->getType();
//            doris::vectorized::write_binary(type, buf);
//
//            switch (type)
//            {
//                case Field::Types::Null: break;
//                case Field::Types::UInt64:
//                {
//                    doris::vectorized::write_var_uint(get<UInt64>(*it), buf);
//                    break;
//                }
//                case Field::Types::UInt128:
//                {
//                    doris::vectorized::write_binary(get<UInt128>(*it), buf);
//                    break;
//                }
//                case Field::Types::Int64:
//                {
//                    doris::vectorized::write_var_int(get<Int64>(*it), buf);
//                    break;
//                }
//                case Field::Types::Float64:
//                {
//                    doris::vectorized::write_float_binary(get<Float64>(*it), buf);
//                    break;
//                }
//                case Field::Types::String:
//                {
//                    doris::vectorized::write_string_binary(get<std::string>(*it), buf);
//                    break;
//                }
//                case Field::Types::Array:
//                {
//                    doris::vectorized::write_binary(get<Array>(*it), buf);
//                    break;
//                }
//                case Field::Types::Tuple:
//                {
//                    doris::vectorized::write_binary(get<Tuple>(*it), buf);
//                    break;
//                }
//                case Field::Types::AggregateFunctionState:
//                {
//                    doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().name, buf);
//                    doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().data, buf);
//                    break;
//                }
//            }
//        }
//    }
//
//    void write_text(const Tuple & x, WriteBuffer & buf)
//    {
//        writeFieldText(doris::vectorized::Field(x), buf);
//    }
//
//    void writeFieldText(const Field & x, WriteBuffer & buf)
//    {
//        doris::vectorized::String res = applyVisitor(doris::vectorized::FieldVisitorToString(), x);
//        buf.write(res.data(), res.size());
//    }
void readBinary(Array& x, std::istream& buf) {
    size_t size;
    UInt8 type;
        doris::vectorized::read_binary(type, buf);
        doris::vectorized::read_binary(size, buf);

    for (size_t index = 0; index < size; ++index) {
        switch (type) {
        case Field::Types::Null: {
            x.push_back(doris::vectorized::Field());
            break;
        }
        case Field::Types::UInt64: {
            UInt64 value;
            doris::vectorized::read_var_uint(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::UInt128: {
            UInt128 value;
            doris::vectorized::read_binary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Int64: {
            Int64 value;
            doris::vectorized::read_var_int(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::Float64: {
            Float64 value;
            doris::vectorized::read_float_binary(value, buf);
            x.push_back(value);
            break;
        }
        case Field::Types::String: {
            std::string value;
            doris::vectorized::read_string_binary(value, buf);
            x.push_back(value);
            break;
        }
        // case Field::Types::Array: {
        //     Array value;
        //     doris::vectorized::read_binary(value, buf);
        //     x.push_back(value);
        //     break;
        // }
        // case Field::Types::Tuple: {
        //     Tuple value;
        //     doris::vectorized::read_binary(value, buf);
        //     x.push_back(value);
        //     break;
        // }
        case Field::Types::AggregateFunctionState: {
            AggregateFunctionStateData value;
            doris::vectorized::read_string_binary(value.name, buf);
            doris::vectorized::read_string_binary(value.data, buf);
            x.push_back(value);
            break;
        }
        }
    }
}

void writeBinary(const Array& x, std::ostream& buf) {
    UInt8 type = Field::Types::Null;
    size_t size = x.size();
    if (size) type = x.front().getType();
    doris::vectorized::writeBinary(type, buf);
    doris::vectorized::writeBinary(size, buf);

    for (Array::const_iterator it = x.begin(); it != x.end(); ++it) {
        switch (type) {
        case Field::Types::Null:
            break;
        case Field::Types::UInt64: {
            doris::vectorized::write_var_uint(get<UInt64>(*it), buf);
            break;
        }
        case Field::Types::UInt128: {
            doris::vectorized::write_binary(get<UInt128>(*it), buf);
            break;
        }
        case Field::Types::Int64: {
            doris::vectorized::write_var_int(get<Int64>(*it), buf);
            break;
        }
        case Field::Types::Float64: {
            doris::vectorized::write_float_binary(get<Float64>(*it), buf);
            break;
        }
        case Field::Types::String: {
            doris::vectorized::write_string_binary(get<std::string>(*it), buf);
            break;
        }
        // case Field::Types::Array: {
        //     doris::vectorized::write_binary(get<Array>(*it), buf);
        //     break;
        // }
        // case Field::Types::Tuple: {
        //     doris::vectorized::write_binary(get<Tuple>(*it), buf);
        //     break;
        // }
        case Field::Types::AggregateFunctionState: {
            doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().name, buf);
            doris::vectorized::write_string_binary(it->get<AggregateFunctionStateData>().data, buf);
            break;
        }
        }
    }
}

template <>
Decimal32 DecimalField<Decimal32>::getScaleMultiplier() const {
    return DataTypeDecimal<Decimal32>::getScaleMultiplier(scale);
}

template <>
Decimal64 DecimalField<Decimal64>::getScaleMultiplier() const {
    return DataTypeDecimal<Decimal64>::getScaleMultiplier(scale);
}

template <>
Decimal128 DecimalField<Decimal128>::getScaleMultiplier() const {
    return DataTypeDecimal<Decimal128>::getScaleMultiplier(scale);
}

template <typename T>
static bool decEqual(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, EqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLess(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <typename T>
static bool decLessOrEqual(T x, T y, UInt32 x_scale, UInt32 y_scale) {
    using Comparator = DecimalComparison<T, T, LessOrEqualsOp>;
    return Comparator::compare(x, y, x_scale, y_scale);
}

template <>
bool decimalEqual(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return decEqual(x, y, xs, ys);
}
template <>
bool decimalLess(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return decLess(x, y, xs, ys);
}
template <>
bool decimalLessOrEqual(Decimal32 x, Decimal32 y, UInt32 xs, UInt32 ys) {
    return decLessOrEqual(x, y, xs, ys);
}

template <>
bool decimalEqual(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return decEqual(x, y, xs, ys);
}
template <>
bool decimalLess(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return decLess(x, y, xs, ys);
}
template <>
bool decimalLessOrEqual(Decimal64 x, Decimal64 y, UInt32 xs, UInt32 ys) {
    return decLessOrEqual(x, y, xs, ys);
}

template <>
bool decimalEqual(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return decEqual(x, y, xs, ys);
}
template <>
bool decimalLess(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return decLess(x, y, xs, ys);
}
template <>
bool decimalLessOrEqual(Decimal128 x, Decimal128 y, UInt32 xs, UInt32 ys) {
    return decLessOrEqual(x, y, xs, ys);
}
} // namespace doris::vectorized
