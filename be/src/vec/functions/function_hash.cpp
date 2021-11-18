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

#include "vec/functions/function_hash.h"

#include "util/hash_util.hpp"
#include "vec/functions/function_variadic_arguments.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
struct MurmurHash2Impl64 {
    static constexpr auto name = "murmurHash2_64";
    using ReturnType = UInt64;

    static ColumnVector<ReturnType>::MutablePtr CreateResultColumn(size_t input_rows_count) {
        return ColumnVector<ReturnType>::create(input_rows_count);
    }

    static Status empty_apply(typename ColumnVector<ReturnType>::Container& vec_to,
                              size_t input_rows_count) {
        vec_to.assign(input_rows_count, static_cast<ReturnType>(0xe28dbde7fe22e41c));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              typename ColumnVector<ReturnType>::Container& vec_to) {
        execute_any<true>(type, column, vec_to);
        return Status::OK();
    }

    static Status combine_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                                typename ColumnVector<ReturnType>::Container& vec_to) {
        execute_any<false>(type, column, vec_to);
        return Status::OK();
    }

    template <typename FromType, bool first>
    static Status execute_int_type(const IColumn* column,
                            typename ColumnVector<ReturnType>::Container& vec_to) {
        if (const ColumnVector<FromType>* col_from =
                    check_and_get_column<ColumnVector<FromType>>(column)) {
            const typename ColumnVector<FromType>::Container& vec_from = col_from->get_data();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i) {
                ReturnType h = HashUtil::murmur_hash2_64(
                        reinterpret_cast<const char*>(reinterpret_cast<const char*>(&vec_from[i])),
                                                      sizeof(vec_from[i]), 0);
                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = IntHash64Impl::apply(vec_to[i]) ^ h;
            }
        } else if (auto col_from_const =
                           check_and_get_column_const<ColumnVector<FromType>>(column)) {
            auto value = col_from_const->template get_value<FromType>();
            ReturnType hash;
            hash = IntHash64Impl::apply(ext::bit_cast<UInt64>(value));

            size_t size = vec_to.size();
            if (first) {
                vec_to.assign(size, hash);
            } else {
                for (size_t i = 0; i < size; ++i) vec_to[i] = IntHash64Impl::apply(vec_to[i]) ^ hash;
            }
        } else {
            DCHECK(false);
            return Status::NotSupported(fmt::format("Illegal column {} of argument of function {}",
                                                    column->get_name(), name));
        }
        return Status::OK();
    }

    template <bool first>
    static Status execute_string(const IColumn* column,
                                 typename ColumnVector<ReturnType>::Container& vec_to) {
        if (const ColumnString* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                const ReturnType h = HashUtil::murmur_hash2_64(
                        reinterpret_cast<const char*>(&data[current_offset]),
                        offsets[i] - current_offset - 1, 0);

                if (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = IntHash64Impl::apply(vec_to[i]) ^ h;

                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            String value = col_from_const->get_value<String>().data();
            const ReturnType hash = HashUtil::murmur_hash2_64(value.data(), value.size(), 0);
            const size_t size = vec_to.size();

            if (first) {
                vec_to.assign(size, hash);
            } else {
                for (size_t i = 0; i < size; ++i) {
                    vec_to[i] = vec_to[i] = IntHash64Impl::apply(vec_to[i]) ^ hash;
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported(fmt::format("Illegal column {} of argument of function {}",
                                                    column->get_name(), name));
        }
        return Status::OK();
    }

    template <bool first>
    static Status execute_any(const IDataType* from_type, const IColumn* icolumn,
                       typename ColumnVector<ReturnType>::Container& vec_to) {
        WhichDataType which(from_type);

        if (which.is_uint8())
            execute_int_type<UInt8, first>(icolumn, vec_to);
        else if (which.is_int16())
            execute_int_type<UInt16, first>(icolumn, vec_to);
        else if (which.is_uint32())
            execute_int_type<UInt32, first>(icolumn, vec_to);
        else if (which.is_uint64())
            execute_int_type<UInt64, first>(icolumn, vec_to);
        else if (which.is_int8())
            execute_int_type<Int8, first>(icolumn, vec_to);
        else if (which.is_int16())
            execute_int_type<Int16, first>(icolumn, vec_to);
        else if (which.is_int32())
            execute_int_type<Int32, first>(icolumn, vec_to);
        else if (which.is_int64())
            execute_int_type<Int64, first>(icolumn, vec_to);
        else if (which.is_float32())
            execute_int_type<Float32, first>(icolumn, vec_to);
        else if (which.is_float64())
            execute_int_type<Float64, first>(icolumn, vec_to);
        else if (which.is_string())
            execute_string<first>(icolumn, vec_to);
        else {
            DCHECK(false);
            return Status::NotSupported(fmt::format("Illegal column {} of argument of function {}",
                                                    icolumn->get_name(), name));
        }
        return Status::OK();
    }
};
using FunctionMurmurHash2_64 = FunctionVariadicArgumentsBase<DataTypeUInt64, MurmurHash2Impl64>;

struct MurmurHash3Impl32 {
    static constexpr auto name = "murmur_hash3_32";
    using ReturnType = Int32;

    static ColumnVector<ReturnType>::MutablePtr CreateResultColumn(size_t input_rows_count) {
        return ColumnVector<ReturnType>::create(input_rows_count);
    }

    static Status empty_apply(typename ColumnVector<ReturnType>::Container& vec_to,
                              size_t input_rows_count) {
        vec_to.assign(input_rows_count, static_cast<ReturnType>(0xe28dbde7fe22e41c));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              typename ColumnVector<ReturnType>::Container& vec_to) {
        return execute<true>(type, column, input_rows_count, vec_to);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                                typename ColumnVector<ReturnType>::Container& vec_to) {
        return execute<false>(type, column, input_rows_count, vec_to);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          typename ColumnVector<ReturnType>::Container& vec_to) {
        if (const ColumnString* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                if (first) {
                    vec_to[i] =
                            HashUtil::murmur_hash3_32(
                                    reinterpret_cast<const char*>(&data[current_offset]),
                                    offsets[i] - current_offset - 1,
                                    HashUtil::MURMUR3_32_SEED);
                } else {
                    vec_to[i] = HashUtil::murmur_hash3_32(
                            reinterpret_cast<const char*>(&data[current_offset]),
                            offsets[i] - current_offset - 1,
                            ext::bit_cast<UInt32>(vec_to[i]));
                }
                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            String value = col_from_const->get_value<String>().data();
            if (first) {
                const ReturnType seed =
                        HashUtil::murmur_hash3_32(
                                value.data(),
                                value.size(),
                                HashUtil::MURMUR3_32_SEED);
                const size_t size = vec_to.size();
                vec_to.assign(size, seed);
            } else {
                for (size_t i = 0; i < vec_to.size(); ++i) {
                    vec_to[i] = HashUtil::murmur_hash3_32(
                            value.data(),
                            value.size(),
                            ext::bit_cast<UInt32>(vec_to[i]));
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported(fmt::format("Illegal column {} of argument of function {}",
                                                    column->get_name(), name));
        }
        return Status::OK();
    }
};
using FunctionMurmurHash3_32 = FunctionVariadicArgumentsBase<DataTypeInt32, MurmurHash3Impl32>;

void register_function_function_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMurmurHash2_64>();
    factory.register_function<FunctionMurmurHash3_32>();
}
} // namespace doris::vectorized