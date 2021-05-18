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

#include <limits>
#include <type_traits>

#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/functions_logical.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int TOO_LARGE_STRING_SIZE;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

/** Comparison functions: ==, !=, <, >, <=, >=.
  * The comparison functions always return 0 or 1 (UInt8).
  *
  * You can compare the following types:
  * - numbers and decimals;
  * - strings and fixed strings;
  * - dates;
  * - datetimes;
  *   within each group, but not from different groups;
  * - tuples (lexicographic comparison).
  *
  * Exception: You can compare the date and datetime with a constant string. Example: EventDate = '2015-01-01'.
  */

template <typename A, typename B, typename Op>
struct NumComparisonImpl {
    /// If you don't specify NO_INLINE, the compiler will inline this function, but we don't need this as this function contains tight loop inside.
    static void NO_INLINE vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                                        PaddedPODArray<UInt8>& c) {
        /** GCC 4.8.2 vectorizes a loop only if it is written in this form.
          * In this case, if you loop through the array index (the code will look simpler),
          *  the loop will not be vectorized.
          */

        size_t size = a.size();
        const A* a_pos = a.data();
        const B* b_pos = b.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = Op::apply(*a_pos, *b_pos);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vector_constant(const PaddedPODArray<A>& a, B b,
                                          PaddedPODArray<UInt8>& c) {
        size_t size = a.size();
        const A* a_pos = a.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = Op::apply(*a_pos, b);
            ++a_pos;
            ++c_pos;
        }
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<UInt8>& c) {
        NumComparisonImpl<B, A, typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(A a, B b, UInt8& c) { c = Op::apply(a, b); }
};

template <typename Op>
struct StringComparisonImpl {
    static void NO_INLINE string_vector_string_vector(const ColumnString::Chars& a_data,
                                                      const ColumnString::Offsets& a_offsets,
                                                      const ColumnString::Chars& b_data,
                                                      const ColumnString::Offsets& b_offsets,
                                                      PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + prev_a_offset,
                                                        a_offsets[i] - prev_a_offset - 1,
                                                        b_data.data() + prev_b_offset,
                                                        b_offsets[i] - prev_b_offset - 1),
                             0);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(const ColumnString::Chars& a_data,
                                                            const ColumnString::Offsets& a_offsets,
                                                            const ColumnString::Chars& b_data,
                                                            ColumnString::Offset b_n,
                                                            PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + prev_a_offset,
                                                        a_offsets[i] - prev_a_offset - 1,
                                                        b_data.data() + i * b_n, b_n),
                             0);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(const ColumnString::Chars& a_data,
                                                 const ColumnString::Offsets& a_offsets,
                                                 const ColumnString::Chars& b_data,
                                                 ColumnString::Offset b_size,
                                                 PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + prev_a_offset,
                                                        a_offsets[i] - prev_a_offset - 1,
                                                        b_data.data(), b_size),
                             0);

            prev_a_offset = a_offsets[i];
        }
    }

    static void fixed_string_vector_string_vector(const ColumnString::Chars& a_data,
                                                  ColumnString::Offset a_n,
                                                  const ColumnString::Chars& b_data,
                                                  const ColumnString::Offsets& b_offsets,
                                                  PaddedPODArray<UInt8>& c) {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_fixed_string_vector(
                b_data, b_offsets, a_data, a_n, c);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
            const ColumnString::Chars& a_data, const ColumnString::Chars& b_data,
            PaddedPODArray<UInt8>& c) {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[i]), 0);
    }

    static void NO_INLINE fixed_string_vector_constant_16(const ColumnString::Chars& a_data,
                                                          const ColumnString::Chars& b_data,
                                                          PaddedPODArray<UInt8>& c) {
        size_t size = a_data.size();

        for (size_t i = 0, j = 0; i < size; i += 16, ++j)
            c[j] = Op::apply(memcmp16(&a_data[i], &b_data[0]), 0);
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector(const ColumnString::Chars& a_data,
                                                                  ColumnString::Offset a_n,
                                                                  const ColumnString::Chars& b_data,
                                                                  ColumnString::Offset b_n,
                                                                  PaddedPODArray<UInt8>& c) {
        if (a_n == 16 && b_n == 16) {
            /** Specialization if both sizes are 16.
              * To more efficient comparison of IPv6 addresses stored in FixedString(16).
              */
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        } else if (a_n == b_n) {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(
                        memcmpSmallAllowOverflow15(a_data.data() + i, b_data.data() + i, a_n), 0);
        } else {
            size_t size = a_data.size() / a_n;

            for (size_t i = 0; i < size; ++i)
                c[i] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + i * a_n, a_n,
                                                            b_data.data() + i * b_n, b_n),
                                 0);
        }
    }

    static void NO_INLINE fixed_string_vector_constant(const ColumnString::Chars& a_data,
                                                       ColumnString::Offset a_n,
                                                       const ColumnString::Chars& b_data,
                                                       ColumnString::Offset b_size,
                                                       PaddedPODArray<UInt8>& c) {
        if (a_n == 16 && b_size == 16) {
            fixed_string_vector_constant_16(a_data, b_data, c);
        } else if (a_n == b_size) {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(memcmpSmallAllowOverflow15(a_data.data() + i, b_data.data(), a_n),
                                 0);
        } else {
            size_t size = a_data.size();
            for (size_t i = 0, j = 0; i < size; i += a_n, ++j)
                c[j] = Op::apply(
                        memcmpSmallAllowOverflow15(a_data.data() + i, a_n, b_data.data(), b_size),
                        0);
        }
    }

    static void constant_string_vector(const ColumnString::Chars& a_data,
                                       ColumnString::Offset a_size,
                                       const ColumnString::Chars& b_data,
                                       const ColumnString::Offsets& b_offsets,
                                       PaddedPODArray<UInt8>& c) {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets,
                                                                               a_data, a_size, c);
    }

    static void constant_fixed_string_vector(const ColumnString::Chars& a_data,
                                             ColumnString::Offset a_size,
                                             const ColumnString::Chars& b_data,
                                             ColumnString::Offset b_n, PaddedPODArray<UInt8>& c) {
        StringComparisonImpl<typename Op::SymmetricOp>::fixed_string_vector_constant(
                b_data, b_n, a_data, a_size, c);
    }

    static void constant_constant(const ColumnString::Chars& a_data, ColumnString::Offset a_size,
                                  const ColumnString::Chars& b_data, ColumnString::Offset b_size,
                                  UInt8& c) {
        c = Op::apply(memcmpSmallAllowOverflow15(a_data.data(), a_size, b_data.data(), b_size), 0);
    }
};

/// Comparisons for equality/inequality are implemented slightly more efficient.
template <bool positive>
struct StringEqualsImpl {
    static void NO_INLINE string_vector_string_vector(const ColumnString::Chars& a_data,
                                                      const ColumnString::Offsets& a_offsets,
                                                      const ColumnString::Chars& b_data,
                                                      const ColumnString::Offsets& b_offsets,
                                                      PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            auto a_size = a_offsets[i] - prev_a_offset - 1;
            auto b_size = b_offsets[i] - prev_b_offset - 1;

            c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + prev_a_offset, a_size,
                                                            b_data.data() + prev_b_offset, b_size);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_fixed_string_vector(const ColumnString::Chars& a_data,
                                                            const ColumnString::Offsets& a_offsets,
                                                            const ColumnString::Chars& b_data,
                                                            ColumnString::Offset b_n,
                                                            PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            auto a_size = a_offsets[i] - prev_a_offset - 1;

            c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + prev_a_offset, a_size,
                                                            b_data.data() + b_n * i, b_n);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(const ColumnString::Chars& a_data,
                                                 const ColumnString::Offsets& a_offsets,
                                                 const ColumnString::Chars& b_data,
                                                 ColumnString::Offset b_size,
                                                 PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            auto a_size = a_offsets[i] - prev_a_offset - 1;

            c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + prev_a_offset, a_size,
                                                            b_data.data(), b_size);

            prev_a_offset = a_offsets[i];
        }
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector_16(
            const ColumnString::Chars& a_data, const ColumnString::Chars& b_data,
            PaddedPODArray<UInt8>& c) {
        size_t size = a_data.size() / 16;

        for (size_t i = 0; i < size; ++i)
            c[i] = positive == memequal16(a_data.data() + i * 16, b_data.data() + i * 16);
    }

    static void NO_INLINE fixed_string_vector_constant_16(const ColumnString::Chars& a_data,
                                                          const ColumnString::Chars& b_data,
                                                          PaddedPODArray<UInt8>& c) {
        size_t size = a_data.size() / 16;

        for (size_t i = 0; i < size; ++i)
            c[i] = positive == memequal16(a_data.data() + i * 16, b_data.data());
    }

    static void NO_INLINE fixed_string_vector_fixed_string_vector(const ColumnString::Chars& a_data,
                                                                  ColumnString::Offset a_n,
                                                                  const ColumnString::Chars& b_data,
                                                                  ColumnString::Offset b_n,
                                                                  PaddedPODArray<UInt8>& c) {
        /** Specialization if both sizes are 16.
          * To more efficient comparison of IPv6 addresses stored in FixedString(16).
          */
        if (a_n == 16 && b_n == 16) {
            fixed_string_vector_fixed_string_vector_16(a_data, b_data, c);
        } else {
            size_t size = a_data.size() / a_n;
            for (size_t i = 0; i < size; ++i)
                c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + i * a_n, a_n,
                                                                b_data.data() + i * b_n, b_n);
        }
    }

    static void NO_INLINE fixed_string_vector_constant(const ColumnString::Chars& a_data,
                                                       ColumnString::Offset a_n,
                                                       const ColumnString::Chars& b_data,
                                                       ColumnString::Offset b_size,
                                                       PaddedPODArray<UInt8>& c) {
        if (a_n == 16 && b_size == 16) {
            fixed_string_vector_constant_16(a_data, b_data, c);
        } else {
            size_t size = a_data.size() / a_n;
            for (size_t i = 0; i < size; ++i)
                c[i] = positive == memequalSmallAllowOverflow15(a_data.data() + i * a_n, a_n,
                                                                b_data.data(), b_size);
        }
    }

    static void fixed_string_vector_string_vector(const ColumnString::Chars& a_data,
                                                  ColumnString::Offset a_n,
                                                  const ColumnString::Chars& b_data,
                                                  const ColumnString::Offsets& b_offsets,
                                                  PaddedPODArray<UInt8>& c) {
        string_vector_fixed_string_vector(b_data, b_offsets, a_data, a_n, c);
    }

    static void constant_string_vector(const ColumnString::Chars& a_data,
                                       ColumnString::Offset a_size,
                                       const ColumnString::Chars& b_data,
                                       const ColumnString::Offsets& b_offsets,
                                       PaddedPODArray<UInt8>& c) {
        string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }

    static void constant_fixed_string_vector(const ColumnString::Chars& a_data,
                                             ColumnString::Offset a_size,
                                             const ColumnString::Chars& b_data,
                                             ColumnString::Offset b_n, PaddedPODArray<UInt8>& c) {
        fixed_string_vector_constant(b_data, b_n, a_data, a_size, c);
    }

    static void constant_constant(const ColumnString::Chars& a_data, ColumnString::Offset a_size,
                                  const ColumnString::Chars& b_data, ColumnString::Offset b_size,
                                  UInt8& c) {
        c = positive == memequalSmallAllowOverflow15(a_data.data(), a_size, b_data.data(), b_size);
    }
};

template <typename A, typename B>
struct StringComparisonImpl<EqualsOp<A, B>> : StringEqualsImpl<true> {};

template <typename A, typename B>
struct StringComparisonImpl<NotEqualsOp<A, B>> : StringEqualsImpl<false> {};

/// Generic version, implemented for columns of same type.
template <typename Op>
struct GenericComparisonImpl {
    static void NO_INLINE vector_vector(const IColumn& a, const IColumn& b,
                                        PaddedPODArray<UInt8>& c) {
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, i, b, 1), 0);
    }

    static void NO_INLINE vector_constant(const IColumn& a, const IColumn& b,
                                          PaddedPODArray<UInt8>& c) {
        auto b_materialized = b.cloneResized(1)->convertToFullColumnIfConst();
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compareAt(i, 0, *b_materialized, 1), 0);
    }

    static void constant_vector(const IColumn& a, const IColumn& b, PaddedPODArray<UInt8>& c) {
        GenericComparisonImpl<typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(const IColumn& a, const IColumn& b, UInt8& c) {
        c = Op::apply(a.compareAt(0, 0, b, 1), 0);
    }
};

#if USE_EMBEDDED_COMPILER

template <template <typename, typename> typename Op>
struct CompileOp;

template <>
struct CompileOp<EqualsOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool /*is_signed*/) {
        return x->getType()->isIntegerTy() ? b.CreateICmpEQ(x, y)
                                           : b.CreateFCmpOEQ(x, y); /// qNaNs always compare false
    }
};

template <>
struct CompileOp<NotEqualsOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool /*is_signed*/) {
        return x->getType()->isIntegerTy() ? b.CreateICmpNE(x, y) : b.CreateFCmpONE(x, y);
    }
};

template <>
struct CompileOp<LessOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool is_signed) {
        return x->getType()->isIntegerTy()
                       ? (is_signed ? b.CreateICmpSLT(x, y) : b.CreateICmpULT(x, y))
                       : b.CreateFCmpOLT(x, y);
    }
};

template <>
struct CompileOp<GreaterOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool is_signed) {
        return x->getType()->isIntegerTy()
                       ? (is_signed ? b.CreateICmpSGT(x, y) : b.CreateICmpUGT(x, y))
                       : b.CreateFCmpOGT(x, y);
    }
};

template <>
struct CompileOp<LessOrEqualsOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool is_signed) {
        return x->getType()->isIntegerTy()
                       ? (is_signed ? b.CreateICmpSLE(x, y) : b.CreateICmpULE(x, y))
                       : b.CreateFCmpOLE(x, y);
    }
};

template <>
struct CompileOp<GreaterOrEqualsOp> {
    static llvm::Value* compile(llvm::IRBuilder<>& b, llvm::Value* x, llvm::Value* y,
                                bool is_signed) {
        return x->getType()->isIntegerTy()
                       ? (is_signed ? b.CreateICmpSGE(x, y) : b.CreateICmpUGE(x, y))
                       : b.CreateFCmpOGE(x, y);
    }
};

#endif

struct NameEquals {
    static constexpr auto name = "eq";
};
struct NameNotEquals {
    static constexpr auto name = "ne";
};
struct NameLess {
    static constexpr auto name = "lt";
};
struct NameGreater {
    static constexpr auto name = "gt";
};
struct NameLessOrEquals {
    static constexpr auto name = "le";
};
struct NameGreaterOrEquals {
    static constexpr auto name = "ge";
};

template <template <typename, typename> class Op, typename Name>
class FunctionComparison : public IFunction {
public:
    static constexpr auto name = Name::name;
    //    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionComparison>(context); }
    static FunctionPtr create() { return std::make_shared<FunctionComparison>(); }

    //    FunctionComparison(const Context & context_)
    //    :   context(context_),
    //        check_decimal_overflow(decimalCheckComparisonOverflow(context))
    FunctionComparison() {}

    bool useDefaultImplementationForNulls() const override { return false; }

private:
    //    const Context & context;
    //    bool check_decimal_overflow = true;

    template <typename T0, typename T1>
    bool executeNumRightType(Block& block, size_t result, const ColumnVector<T0>* col_left,
                             const IColumn* col_right_untyped) {
        if (const ColumnVector<T1>* col_right =
                    checkAndGetColumn<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->getData();
            vec_res.resize(col_left->getData().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->getData(),
                                                                 col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        } else if (auto col_right_const =
                           checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_constant(
                    col_left->getData(), col_right_const->template getValue<T1>(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        }

        return false;
    }

    template <typename T0, typename T1>
    bool executeNumConstRightType(Block& block, size_t result, const ColumnConst* col_left,
                                  const IColumn* col_right_untyped) {
        if (const ColumnVector<T1>* col_right =
                    checkAndGetColumn<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->getData();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_vector(
                    col_left->template getValue<T0>(), col_right->getData(), vec_res);

            block.getByPosition(result).column = std::move(col_res);
            return true;
        } else if (auto col_right_const =
                           checkAndGetColumnConst<ColumnVector<T1>>(col_right_untyped)) {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_constant(
                    col_left->template getValue<T0>(), col_right_const->template getValue<T1>(),
                    res);

            block.getByPosition(result).column =
                    DataTypeUInt8().createColumnConst(col_left->size(), toField(res));
            return true;
        }

        return false;
    }

    template <typename T0>
    bool executeNumLeftType(Block& block, size_t result, const IColumn* col_left_untyped,
                            const IColumn* col_right_untyped) {
        if (const ColumnVector<T0>* col_left =
                    checkAndGetColumn<ColumnVector<T0>>(col_left_untyped)) {
            if (executeNumRightType<T0, UInt8>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, UInt16>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, UInt32>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, UInt64>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, UInt128>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Int8>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Int16>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Int32>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Int64>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Int128>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Float32>(block, result, col_left, col_right_untyped) ||
                executeNumRightType<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName() +
                                        " of second argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        } else if (auto col_left_const =
                           checkAndGetColumnConst<ColumnVector<T0>>(col_left_untyped)) {
            if (executeNumConstRightType<T0, UInt8>(block, result, col_left_const,
                                                    col_right_untyped) ||
                executeNumConstRightType<T0, UInt16>(block, result, col_left_const,
                                                     col_right_untyped) ||
                executeNumConstRightType<T0, UInt32>(block, result, col_left_const,
                                                     col_right_untyped) ||
                executeNumConstRightType<T0, UInt64>(block, result, col_left_const,
                                                     col_right_untyped) ||
                executeNumConstRightType<T0, UInt128>(block, result, col_left_const,
                                                      col_right_untyped) ||
                executeNumConstRightType<T0, Int8>(block, result, col_left_const,
                                                   col_right_untyped) ||
                executeNumConstRightType<T0, Int16>(block, result, col_left_const,
                                                    col_right_untyped) ||
                executeNumConstRightType<T0, Int32>(block, result, col_left_const,
                                                    col_right_untyped) ||
                executeNumConstRightType<T0, Int64>(block, result, col_left_const,
                                                    col_right_untyped) ||
                executeNumConstRightType<T0, Int128>(block, result, col_left_const,
                                                     col_right_untyped) ||
                executeNumConstRightType<T0, Float32>(block, result, col_left_const,
                                                      col_right_untyped) ||
                executeNumConstRightType<T0, Float64>(block, result, col_left_const,
                                                      col_right_untyped))
                return true;
            else
                throw Exception("Illegal column " + col_right_untyped->getName() +
                                        " of second argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        }

        return false;
    }

    void executeDecimal(Block& block, size_t result, const ColumnWithTypeAndName& col_left,
                        const ColumnWithTypeAndName& col_right) {
        TypeIndex left_number = col_left.type->getTypeId();
        TypeIndex right_number = col_right.type->getTypeId();

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            //            if (check_decimal_overflow)
            //                DecimalComparison<LeftDataType, RightDataType, Op, true>(block, result, col_left, col_right);
            //            else
            DecimalComparison<LeftDataType, RightDataType, Op, false>(block, result, col_left,
                                                                      col_right);
            return true;
        };

        if (!callOnBasicTypes<true, false, true, false>(left_number, right_number, call))
            throw Exception("Wrong call for " + getName() + " with " + col_left.type->getName() +
                                    " and " + col_right.type->getName(),
                            ErrorCodes::LOGICAL_ERROR);
    }

    bool executeString(Block& block, size_t result, const IColumn* c0, const IColumn* c1) {
        const ColumnString* c0_string = checkAndGetColumn<ColumnString>(c0);
        const ColumnString* c1_string = checkAndGetColumn<ColumnString>(c1);
        //        const ColumnFixedString * c0_fixed_string = checkAndGetColumn<ColumnFixedString>(c0);
        //        const ColumnFixedString * c1_fixed_string = checkAndGetColumn<ColumnFixedString>(c1);

        const ColumnConst* c0_const = checkAndGetColumnConstStringOrFixedString(c0);
        const ColumnConst* c1_const = checkAndGetColumnConstStringOrFixedString(c1);

        //        if (!((c0_string || c0_fixed_string || c0_const) && (c1_string || c1_fixed_string || c1_const)))
        if (!((c0_string || c0_const) && (c1_string || c1_const))) return false;

        const ColumnString::Chars* c0_const_chars = nullptr;
        const ColumnString::Chars* c1_const_chars = nullptr;
        ColumnString::Offset c0_const_size = 0;
        ColumnString::Offset c1_const_size = 0;

        if (c0_const) {
            const ColumnString* c0_const_string =
                    checkAndGetColumn<ColumnString>(&c0_const->getDataColumn());
            //            const ColumnFixedString * c0_const_fixed_string = checkAndGetColumn<ColumnFixedString>(&c0_const->getDataColumn());

            if (c0_const_string) {
                c0_const_chars = &c0_const_string->getChars();
                c0_const_size = c0_const_string->getDataAt(0).size;
            }
            //            else if (c0_const_fixed_string)
            //            {
            //                c0_const_chars = &c0_const_fixed_string->getChars();
            //                c0_const_size = c0_const_fixed_string->getN();
            //            }
            else
                throw Exception(
                        "Logical error: ColumnConst contains not String nor FixedString column",
                        ErrorCodes::ILLEGAL_COLUMN);
        }

        if (c1_const) {
            const ColumnString* c1_const_string =
                    checkAndGetColumn<ColumnString>(&c1_const->getDataColumn());
            //            const ColumnFixedString * c1_const_fixed_string = checkAndGetColumn<ColumnFixedString>(&c1_const->getDataColumn());

            if (c1_const_string) {
                c1_const_chars = &c1_const_string->getChars();
                c1_const_size = c1_const_string->getDataAt(0).size;
            }
            //            else if (c1_const_fixed_string)
            //            {
            //                c1_const_chars = &c1_const_fixed_string->getChars();
            //                c1_const_size = c1_const_fixed_string->getN();
            //            }
            else
                throw Exception(
                        "Logical error: ColumnConst contains not String nor FixedString column",
                        ErrorCodes::ILLEGAL_COLUMN);
        }

        using StringImpl = StringComparisonImpl<Op<int, int>>;

        if (c0_const && c1_const) {
            UInt8 res = 0;
            StringImpl::constant_constant(*c0_const_chars, c0_const_size, *c1_const_chars,
                                          c1_const_size, res);
            block.getByPosition(result).column =
                    block.getByPosition(result).type->createColumnConst(c0_const->size(),
                                                                        toField(res));
            return true;
        } else {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container& vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_string && c1_string)
                StringImpl::string_vector_string_vector(
                        c0_string->getChars(), c0_string->getOffsets(), c1_string->getChars(),
                        c1_string->getOffsets(), c_res->getData());
            //            else if (c0_string && c1_fixed_string)
            //                StringImpl::string_vector_fixed_string_vector(
            //                    c0_string->getChars(), c0_string->getOffsets(),
            //                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
            //                    c_res->getData());
            else if (c0_string && c1_const)
                StringImpl::string_vector_constant(c0_string->getChars(), c0_string->getOffsets(),
                                                   *c1_const_chars, c1_const_size,
                                                   c_res->getData());
            //            else if (c0_fixed_string && c1_string)
            //                StringImpl::fixed_string_vector_string_vector(
            //                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
            //                    c1_string->getChars(), c1_string->getOffsets(),
            //                    c_res->getData());
            //            else if (c0_fixed_string && c1_fixed_string)
            //                StringImpl::fixed_string_vector_fixed_string_vector(
            //                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
            //                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
            //                    c_res->getData());
            //            else if (c0_fixed_string && c1_const)
            //                StringImpl::fixed_string_vector_constant(
            //                    c0_fixed_string->getChars(), c0_fixed_string->getN(),
            //                    *c1_const_chars, c1_const_size,
            //                    c_res->getData());
            else if (c0_const && c1_string)
                StringImpl::constant_string_vector(*c0_const_chars, c0_const_size,
                                                   c1_string->getChars(), c1_string->getOffsets(),
                                                   c_res->getData());
            //            else if (c0_const && c1_fixed_string)
            //                StringImpl::constant_fixed_string_vector(
            //                    *c0_const_chars, c0_const_size,
            //                    c1_fixed_string->getChars(), c1_fixed_string->getN(),
            //                    c_res->getData());
            else
                throw Exception("Illegal columns " + c0->getName() + " and " + c1->getName() +
                                        " of arguments of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);

            block.getByPosition(result).column = std::move(c_res);
            return true;
        }
    }

    void executeGenericIdenticalTypes(Block& block, size_t result, const IColumn* c0,
                                      const IColumn* c1) {
        bool c0_const = isColumnConst(*c0);
        bool c1_const = isColumnConst(*c1);

        if (c0_const && c1_const) {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constant_constant(*c0, *c1, res);
            block.getByPosition(result).column =
                    DataTypeUInt8().createColumnConst(c0->size(), toField(res));
        } else {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container& vec_res = c_res->getData();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constant_vector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vector_constant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vector_vector(*c0, *c1, vec_res);

            block.getByPosition(result).column = std::move(c_res);
        }
    }

    void executeGeneric(Block& block, size_t result, const ColumnWithTypeAndName& c0,
                        const ColumnWithTypeAndName& c1) {
        DataTypePtr common_type = getLeastSupertype({c0.type, c1.type});
        // TODO: Support full castColumn
        //        ColumnPtr c0_converted = castColumn(c0, common_type, context);
        //        ColumnPtr c1_converted = castColumn(c1, common_type, context);

        ColumnPtr c0_converted = castColumnNullable(c0, common_type);
        ColumnPtr c1_converted = castColumnNullable(c1, common_type);

        executeGenericIdenticalTypes(block, result, c0_converted.get(), c1_converted.get());
    }

private:
    ColumnPtr castColumnNullable(const ColumnWithTypeAndName& arg, const DataTypePtr& type) {
        if (arg.type->equals(*type)) return arg.column;

        auto bool_column = ColumnUInt8::create();
        bool_column->insertManyDefaults(arg.column->size());
        return doris::vectorized::ColumnNullable::create(arg.column, bool_column->getPtr());
    }

public:
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override {
        WhichDataType left(arguments[0].get());
        WhichDataType right(arguments[1].get());

        //        const DataTypeTuple * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        //        const DataTypeTuple * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].get());

        //        bool both_represented_by_number = arguments[0]->isValueRepresentedByNumber() && arguments[1]->isValueRepresentedByNumber();
        //        bool has_date = left.isDate() || right.isDate();

        //        if (!((both_represented_by_number && !has_date)   /// Do not allow compare date and number.
        //            || (left.isStringOrFixedString() && right.isStringOrFixedString())
        //            || (left.isDate() && right.isDate())
        //            || (left.isDate() && right.isString())    /// You can compare the date, datetime and an enumeration with a constant string.
        //            || (left.isString() && right.isDate())
        //            || (left.isDateTime() && right.isDateTime())
        //            || (left.isDateTime() && right.isString())
        //            || (left.isString() && right.isDateTime())
        //            || (left.isUUID() && right.isUUID())
        //            || (left.isUUID() && right.isString())
        //            || (left.isString() && right.isUUID())
        //            || (left.isEnum() && right.isEnum() && arguments[0]->getName() == arguments[1]->getName()) /// only equivalent enum type values can be compared against
        //            || (left.isEnum() && right.isString())
        //            || (left.isString() && right.isEnum())
        //            || (left_tuple && right_tuple && left_tuple->getElements().size() == right_tuple->getElements().size())
        //            || (arguments[0]->equals(*arguments[1]))))
        //        {
        //            try
        //            {
        //                getLeastSupertype(arguments);
        //            }
        //            catch (const Exception &)
        //            {
        //                throw Exception("Illegal types of arguments (" + arguments[0]->getName() + ", " + arguments[1]->getName() + ")"
        //                    " of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        //            }
        //        }

        //        if (left_tuple && right_tuple)
        //        {
        //            size_t size = left_tuple->getElements().size();
        //            for (size_t i = 0; i < size; ++i)
        //            {
        //                ColumnsWithTypeAndName args = {{nullptr, left_tuple->getElements()[i], ""},
        //                                               {nullptr, right_tuple->getElements()[i], ""}};
        //                getReturnType(args);
        //            }
        //        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                     size_t input_rows_count) override {
        const auto& col_with_type_and_name_left = block.getByPosition(arguments[0]);
        const auto& col_with_type_and_name_right = block.getByPosition(arguments[1]);
        const IColumn* col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn* col_right_untyped = col_with_type_and_name_right.column.get();

        const DataTypePtr& left_type = col_with_type_and_name_left.type;
        const DataTypePtr& right_type = col_with_type_and_name_right.type;

        /// The case when arguments are the same (tautological comparison). Return constant.
        /// NOTE: Nullable types are special case. (BTW, this function use default implementation for Nullable, so Nullable types cannot be here. Check just in case.)
        /// NOTE: We consider NaN comparison to be implementation specific (and in our implementation NaNs are sometimes equal sometimes not).
        if (left_type->equals(*right_type) && !left_type->isNullable() &&
            col_left_untyped == col_right_untyped) {
            /// Always true: =, <=, >=
            if constexpr (std::is_same_v<Op<int, int>, EqualsOp<int, int>> ||
                          std::is_same_v<Op<int, int>, LessOrEqualsOp<int, int>> ||
                          std::is_same_v<Op<int, int>, GreaterOrEqualsOp<int, int>>) {
                block.getByPosition(result).column =
                        DataTypeUInt8().createColumnConst(input_rows_count, 1u);
                return;
            } else {
                block.getByPosition(result).column =
                        DataTypeUInt8().createColumnConst(input_rows_count, 0u);
                return;
            }
        }

        WhichDataType which_left{left_type};
        WhichDataType which_right{right_type};

        const bool left_is_num = col_left_untyped->isNumeric();
        const bool right_is_num = col_right_untyped->isNumeric();

        bool date_and_datetime = (left_type != right_type) && which_left.isDateOrDateTime() &&
                                 which_right.isDateOrDateTime();

        if (left_is_num && right_is_num && !date_and_datetime) {
            if (!(executeNumLeftType<UInt8>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<UInt16>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<UInt32>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<UInt64>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<UInt128>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Int8>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Int16>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Int32>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Int64>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Int128>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Float32>(block, result, col_left_untyped, col_right_untyped) ||
                  executeNumLeftType<Float64>(block, result, col_left_untyped, col_right_untyped)))
                throw Exception("Illegal column " + col_left_untyped->getName() +
                                        " of first argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN);
        }
        //        else if (checkAndGetDataType<DataTypeTuple>(left_type.get()))
        //        {
        //            executeTuple(block, result, col_with_type_and_name_left, col_with_type_and_name_right, input_rows_count);
        //        }
        else if (isDecimal(left_type) || isDecimal(right_type)) {
            if (!allowDecimalComparison(left_type, right_type))
                throw Exception("No operation " + getName() + " between " + left_type->getName() +
                                        " and " + right_type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            executeDecimal(block, result, col_with_type_and_name_left,
                           col_with_type_and_name_right);
        }
        //        else if (!left_is_num && !right_is_num && executeString(block, result, col_left_untyped, col_right_untyped))
        //        {
        //        }
        //        else if (left_type->equals(*right_type))
        //        {
        //            executeGenericIdenticalTypes(block, result, col_left_untyped, col_right_untyped);
        //        }
        //        else if (executeDateOrDateTimeOrEnumOrUUIDWithConstString(
        //                block, result, col_left_untyped, col_right_untyped,
        //                left_type, right_type,
        //                left_is_num, input_rows_count))
        //        {
        //        }
        else {
            executeGeneric(block, result, col_with_type_and_name_left,
                           col_with_type_and_name_right);
        }
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilableImpl(const DataTypes& types) const override {
        auto isBigInteger = &typeIsEither<DataTypeInt64, DataTypeUInt64, DataTypeUUID>;
        auto isFloatingPoint = &typeIsEither<DataTypeFloat32, DataTypeFloat64>;
        if ((isBigInteger(*types[0]) && isFloatingPoint(*types[1])) ||
            (isBigInteger(*types[1]) && isFloatingPoint(*types[0])) ||
            (WhichDataType(types[0]).isDate() && WhichDataType(types[1]).isDateTime()) ||
            (WhichDataType(types[1]).isDate() && WhichDataType(types[0]).isDateTime()))
            return false; /// TODO: implement (double, int_N where N > double's mantissa width)
        return isCompilableType(types[0]) && isCompilableType(types[1]);
    }

    llvm::Value* compileImpl(llvm::IRBuilderBase& builder, const DataTypes& types,
                             ValuePlaceholders values) const override {
        auto& b = static_cast<llvm::IRBuilder<>&>(builder);
        auto* x = values[0]();
        auto* y = values[1]();
        if (!types[0]->equals(*types[1])) {
            llvm::Type* common;
            if (x->getType()->isIntegerTy() && y->getType()->isIntegerTy())
                common = b.getIntNTy(std::max(
                        /// if one integer has a sign bit, make sure the other does as well. llvm generates optimal code
                        /// (e.g. uses overflow flag on x86) for (word size + 1)-bit integer operations.
                        x->getType()->getIntegerBitWidth() +
                                (!typeIsSigned(*types[0]) && typeIsSigned(*types[1])),
                        y->getType()->getIntegerBitWidth() +
                                (!typeIsSigned(*types[1]) && typeIsSigned(*types[0]))));
            else
                /// (double, float) or (double, int_N where N <= double's mantissa width) -> double
                common = b.getDoubleTy();
            x = nativeCast(b, types[0], x, common);
            y = nativeCast(b, types[1], y, common);
        }
        auto* result =
                CompileOp<Op>::compile(b, x, y, typeIsSigned(*types[0]) || typeIsSigned(*types[1]));
        return b.CreateSelect(result, b.getInt8(1), b.getInt8(0));
    }
#endif
};

} // namespace doris::vectorized
