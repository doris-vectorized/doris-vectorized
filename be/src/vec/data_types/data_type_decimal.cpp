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

#include "vec/data_types/data_type_decimal.h"

#include <type_traits>

#include "gen_cpp/data.pb.h"
#include "vec/common/assert_cast.h"
#include "vec/common/int_exp.h"
#include "vec/common/typeid_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ARGUMENT_OUT_OF_BOUND;
} // namespace ErrorCodes

template <typename T>
std::string DataTypeDecimal<T>::doGetName() const {
    std::stringstream ss;
    ss << "Decimal(" << precision << ", " << scale << ")";
    return ss.str();
}

template <typename T>
bool DataTypeDecimal<T>::equals(const IDataType& rhs) const {
    if (auto* ptype = typeid_cast<const DataTypeDecimal<T>*>(&rhs))
        return scale == ptype->getScale();
    return false;
}

template <typename T>
std::string DataTypeDecimal<T>::to_string(const IColumn& column, size_t row_num) const {
    T value = assert_cast<const ColumnType&>(*column.convertToFullColumnIfConst().get())
                      .getData()[row_num];
    std::ostringstream buf;
    writeText(value, scale, buf);
    return buf.str();
}

template <typename T>
void DataTypeDecimal<T>::serialize(const IColumn& column, PColumn* pcolumn) const {
    std::ostringstream buf;
    for (size_t i = 0; i < column.size(); ++i) {
        const FieldType& x =
                assert_cast<const ColumnType&>(*column.convertToFullColumnIfConst().get())
                        .getElement(i);
        writeBinary(x, buf);
    }

    write_binary(buf, pcolumn);
    pcolumn->mutable_decimal_param()->set_precision(precision);
    pcolumn->mutable_decimal_param()->set_scale(scale);
}

template <typename T>
void DataTypeDecimal<T>::deserialize(const PColumn& pcolumn, IColumn* column) const {
    std::string uncompressed;
    read_binary(pcolumn, &uncompressed);
    std::istringstream istr(uncompressed);
    while (istr.peek() != EOF) {
        typename FieldType::NativeType x;
        readBinary(x, istr);
        assert_cast<ColumnType*>(column)->getData().push_back(FieldType(x));
    }
}

template <typename T>
Field DataTypeDecimal<T>::getDefault() const {
    return DecimalField(T(0), scale);
}

template <typename T>
DataTypePtr DataTypeDecimal<T>::promoteNumericType() const {
    using PromotedType = DataTypeDecimal<Decimal128>;
    return std::make_shared<PromotedType>(PromotedType::maxPrecision(), scale);
}

template <typename T>
MutableColumnPtr DataTypeDecimal<T>::createColumn() const {
    return ColumnType::create(0, scale);
}

//

DataTypePtr createDecimal(UInt64 precision_value, UInt64 scale_value) {
    if (precision_value < minDecimalPrecision() ||
        precision_value > maxDecimalPrecision<Decimal128>())
        throw Exception("Wrong precision", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<UInt64>(scale_value) > precision_value)
        throw Exception("Negative scales and scales larger than precision are not supported",
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (precision_value <= maxDecimalPrecision<Decimal32>())
        return std::make_shared<DataTypeDecimal<Decimal32>>(precision_value, scale_value);
    else if (precision_value <= maxDecimalPrecision<Decimal64>())
        return std::make_shared<DataTypeDecimal<Decimal64>>(precision_value, scale_value);
    return std::make_shared<DataTypeDecimal<Decimal128>>(precision_value, scale_value);
}

template <>
Decimal32 DataTypeDecimal<Decimal32>::getScaleMultiplier(UInt32 scale_) {
    return common::exp10_i32(scale_);
}

template <>
Decimal64 DataTypeDecimal<Decimal64>::getScaleMultiplier(UInt32 scale_) {
    return common::exp10_i64(scale_);
}

template <>
Decimal128 DataTypeDecimal<Decimal128>::getScaleMultiplier(UInt32 scale_) {
    return common::exp10_i128(scale_);
}

/// Explicit template instantiations.
template class DataTypeDecimal<Decimal32>;
template class DataTypeDecimal<Decimal64>;
template class DataTypeDecimal<Decimal128>;

} // namespace doris::vectorized
