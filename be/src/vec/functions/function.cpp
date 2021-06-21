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

#include "vec/functions/function.h"
//#include <vec/Common/config.h>
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
//#include <vec/Common/LRUCache.h>
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
//#include <vec/Columns/ColumnArray.h>
//#include <vec/Columns/ColumnTuple.h>
//#include <vec/Columns/ColumnLowCardinality.h>
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
//#include <vec/DataTypes/DataTypeNullable.h>
//#include <vec/DataTypes/DataTypeTuple.h>
//#include <vec/DataTypes/Native.h>
//#include <vec/DataTypes/DataTypeLowCardinality.h>
//#include <vec/DataTypes/getLeastSupertype.h>
#include "vec/functions/function_helpers.h"
//#include <Interpreters/ExpressionActions.h>
//#include <IO/WriteHelpers.h>
//#include <ext/range.h>
//#include <ext/collection_cast.h>
#include <cstdlib>
#include <memory>
#include <optional>

#if USE_EMBEDDED_COMPILER
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <llvm/IR/IRBuilder.h>
#pragma GCC diagnostic pop
#endif

namespace doris::vectorized {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_COLUMN;
} // namespace ErrorCodes

/// Cache for functions result if it was executed on low cardinality column.
/// It's LRUCache which stores function result executed on dictionary and index mapping.
/// It's expected that cache_size is a number of reading streams (so, will store single cached value per thread).
//class PreparedFunctionLowCardinalityResultCache
//{
//public:
//    /// Will assume that dictionaries with same hash has the same keys.
//    /// Just in case, check that they have also the same size.
//    struct DictionaryKey
//    {
//        UInt128 hash;
//        UInt64 size;
//
//        bool operator== (const DictionaryKey & other) const { return hash == other.hash && size == other.size; }
//    };
//
//    struct DictionaryKeyHash
//    {
//        size_t operator()(const DictionaryKey & key) const
//        {
//            SipHash hash;
//            hash.update(key.hash.low);
//            hash.update(key.hash.high);
//            hash.update(key.size);
//            return hash.get64();
//        }
//    };
//
//    struct CachedValues
//    {
//        /// Store ptr to dictionary to be sure it won't be deleted.
//        ColumnPtr dictionary_holder;
//        ColumnUniquePtr function_result;
//        /// Remap positions. new_pos = index_mapping->index(old_pos);
//        ColumnPtr index_mapping;
//    };
//
//    using CachedValuesPtr = std::shared_ptr<CachedValues>;
//
//    explicit PreparedFunctionLowCardinalityResultCache(size_t cache_size) : cache(cache_size) {}
//
//    CachedValuesPtr get(const DictionaryKey & key) { return cache.get(key); }
//    void set(const DictionaryKey & key, const CachedValuesPtr & mapped) { cache.set(key, mapped); }
//    CachedValuesPtr getOrSet(const DictionaryKey & key, const CachedValuesPtr & mapped)
//    {
//        return cache.getOrSet(key, [&]() { return mapped; }).first;
//    }
//
//private:
//    using Cache = LRUCache<DictionaryKey, CachedValues, DictionaryKeyHash>;
//    Cache cache;
//};

void PreparedFunctionImpl::createLowCardinalityResultCache(size_t cache_size) {
    //    if (!low_cardinality_result_cache)
    //        low_cardinality_result_cache = std::make_shared<PreparedFunctionLowCardinalityResultCache>(cache_size);
}

ColumnPtr wrapInNullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                         size_t result, size_t input_rows_count) {
    ColumnPtr result_null_map_column;

    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;

    if (src->only_null())
        return src;
    else if (auto* nullable = check_and_get_column<ColumnNullable>(*src)) {
        src_not_nullable = nullable->get_nested_column_ptr();
        result_null_map_column = nullable->get_null_map_column_ptr();
    }

    for (const auto& arg : args) {
        const ColumnWithTypeAndName& elem = block.get_by_position(arg);
        if (!elem.type->is_nullable()) continue;

        /// Const Nullable that are NULL.
        if (elem.column->only_null())
            return block.get_by_position(result).type->createColumnConst(input_rows_count, Null());

        if (is_column_const(*elem.column)) continue;

        if (auto* nullable = check_and_get_column<ColumnNullable>(*elem.column)) {
            const ColumnPtr& null_map_column = nullable->get_null_map_column_ptr();
            if (!result_null_map_column) {
                result_null_map_column = null_map_column;
            } else {
                MutableColumnPtr mutable_result_null_map_column =
                        (*std::move(result_null_map_column)).mutate();

                NullMap& result_null_map =
                        assert_cast<ColumnUInt8&>(*mutable_result_null_map_column).get_data();
                const NullMap& src_null_map =
                        assert_cast<const ColumnUInt8&>(*null_map_column).get_data();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    if (src_null_map[i]) result_null_map[i] = 1;

                result_null_map_column = std::move(mutable_result_null_map_column);
            }
        }
    }

    if (!result_null_map_column) return make_nullable(src);

    return ColumnNullable::create(src_not_nullable->convert_to_full_column_if_const(),
                                  result_null_map_column);
}

namespace {

struct NullPresence {
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const Block& block, const ColumnNumbers& args) {
    NullPresence res;

    for (const auto& arg : args) {
        const auto& elem = block.get_by_position(arg);

        if (!res.has_nullable) res.has_nullable = elem.type->is_nullable();
        if (!res.has_null_constant) res.has_null_constant = elem.type->only_null();
    }

    return res;
}

[[maybe_unused]] NullPresence getNullPresense(const ColumnsWithTypeAndName& args) {
    NullPresence res;

    for (const auto& elem : args) {
        if (!res.has_nullable) res.has_nullable = elem.type->is_nullable();
        if (!res.has_null_constant) res.has_null_constant = elem.type->only_null();
    }

    return res;
}

bool allArgumentsAreConstants(const Block& block, const ColumnNumbers& args) {
    for (auto arg : args)
        if (!is_column_const(*block.get_by_position(arg).column)) return false;
    return true;
}
} // namespace

Status PreparedFunctionImpl::defaultImplementationForConstantArguments(
        Block& block, const ColumnNumbers& args, size_t result, size_t input_rows_count,
        bool dry_run, bool* executed) {
    *executed = false;
    ColumnNumbers arguments_to_remain_constants = getArgumentsThatAreAlwaysConstant();

    /// Check that these arguments are really constant.
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < args.size() && !is_column_const(*block.get_by_position(args[arg_num]).column)) {
            return Status::RuntimeError(fmt::format(
                    "Argument at index {} for function {}  must be constant", arg_num, get_name()));
        }

    if (args.empty() || !useDefaultImplementationForConstants() ||
        !allArgumentsAreConstants(block, args))
        return Status::OK();

    Block temporary_block;
    bool have_converted_columns = false;

    size_t arguments_size = args.size();
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num) {
        const ColumnWithTypeAndName& column = block.get_by_position(args[arg_num]);

        if (arguments_to_remain_constants.end() != std::find(arguments_to_remain_constants.begin(),
                                                             arguments_to_remain_constants.end(),
                                                             arg_num)) {
            temporary_block.insert({column.column->clone_resized(1), column.type, column.name});
        } else {
            have_converted_columns = true;
            temporary_block.insert(
                    {assert_cast<const ColumnConst*>(column.column.get())->get_data_column_ptr(),
                     column.type, column.name});
        }
    }

    /** When using default implementation for constants, the function requires at least one argument
      *  not in "arguments_to_remain_constants" set. Otherwise we get infinite recursion.
      */
    if (!have_converted_columns) {
        return Status::RuntimeError(
                fmt::format("Number of arguments for function {} doesn't match: the function "
                            "requires more arguments",
                            get_name()));
    }

    temporary_block.insert(block.get_by_position(result));

    ColumnNumbers temporary_argument_numbers(arguments_size);
    for (size_t i = 0; i < arguments_size; ++i) temporary_argument_numbers[i] = i;

    RETURN_IF_ERROR(executeWithoutLowCardinalityColumns(temporary_block, temporary_argument_numbers,
                                                        arguments_size, temporary_block.rows(),
                                                        dry_run));

    ColumnPtr result_column;
    /// extremely rare case, when we have function with completely const arguments
    /// but some of them produced by non isDeterministic function
    if (temporary_block.get_by_position(arguments_size).column->size() > 1)
        result_column = temporary_block.get_by_position(arguments_size).column->clone_resized(1);
    else
        result_column = temporary_block.get_by_position(arguments_size).column;

    block.get_by_position(result).column = ColumnConst::create(result_column, input_rows_count);
    *executed = true;
    return Status::OK();
}

Status PreparedFunctionImpl::defaultImplementationForNulls(Block& block, const ColumnNumbers& args,
                                                           size_t result, size_t input_rows_count,
                                                           bool dry_run, bool* executed) {
    *executed = false;
    if (args.empty() || !useDefaultImplementationForNulls()) return Status::OK();

    NullPresence null_presence = getNullPresense(block, args);

    if (null_presence.has_null_constant) {
        block.get_by_position(result).column =
                block.get_by_position(result).type->createColumnConst(input_rows_count, Null());
        *executed = true;
        return Status::OK();
    }

    if (null_presence.has_nullable) {
        Block temporary_block = createBlockWithNestedColumns(block, args, result);
        RETURN_IF_ERROR(executeWithoutLowCardinalityColumns(temporary_block, args, result,
                                                            temporary_block.rows(), dry_run));
        block.get_by_position(result).column =
                wrapInNullable(temporary_block.get_by_position(result).column, block, args, result,
                               input_rows_count);
        *executed = true;
        return Status::OK();
    }
    *executed = false;
    return Status::OK();
}

Status PreparedFunctionImpl::executeWithoutLowCardinalityColumns(Block& block,
                                                                 const ColumnNumbers& args,
                                                                 size_t result,
                                                                 size_t input_rows_count,
                                                                 bool dry_run) {
    bool executed = false;
    RETURN_IF_ERROR(defaultImplementationForConstantArguments(block, args, result, input_rows_count,
                                                              dry_run, &executed));
    if (executed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(defaultImplementationForNulls(block, args, result, input_rows_count, dry_run,
                                                  &executed));
    if (executed) {
        return Status::OK();
    }

    if (dry_run)
        return executeImplDryRun(block, args, result, input_rows_count);
    else
        return executeImpl(block, args, result, input_rows_count);
}

//static const ColumnLowCardinality * findLowCardinalityArgument(const Block & block, const ColumnNumbers & args)
//{
//    const ColumnLowCardinality * result_column = nullptr;
//
//    for (auto arg : args)
//    {
//        const ColumnWithTypeAndName & column = block.get_by_position(arg);
//        if (auto * low_cardinality_column = check_and_get_column<ColumnLowCardinality>(column.column.get()))
//        {
//            if (result_column)
//                throw Exception("Expected single dictionary argument for function.", ErrorCodes::LOGICAL_ERROR);
//
//            result_column = low_cardinality_column;
//        }
//    }
//
//    return result_column;
//}

[[maybe_unused]] static ColumnPtr replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
        Block& block, const ColumnNumbers& args, bool can_be_executed_on_default_arguments,
        size_t input_rows_count) {
    size_t num_rows = input_rows_count;
    ColumnPtr indexes;

    /// Change size of constants.
    for (auto arg : args) {
        ColumnWithTypeAndName& column = block.get_by_position(arg);
        if (auto* column_const = check_and_get_column<ColumnConst>(column.column.get())) {
            column.column = column_const->remove_low_cardinality()->clone_resized(num_rows);
            //            column.type = remove_low_cardinality(column.type);
        }
    }

#ifndef NDEBUG
    block.check_number_of_rows(true);
#endif

    return indexes;
}

//static void convertLowCardinalityColumnsToFull(Block & block, const ColumnNumbers & args)
//{
//    for (auto arg : args)
//    {
//        ColumnWithTypeAndName & column = block.get_by_position(arg);
//
//        column.column = recursiveRemoveLowCardinality(column.column);
//        column.type = recursiveRemoveLowCardinality(column.type);
//    }
//}

Status PreparedFunctionImpl::execute(Block& block, const ColumnNumbers& args, size_t result,
                                     size_t input_rows_count, bool dry_run) {
    if (useDefaultImplementationForLowCardinalityColumns()) {
        auto& res = block.safe_get_by_position(result);
        Block block_without_low_cardinality = block.clone_without_columns();

        for (auto arg : args)
            block_without_low_cardinality.safe_get_by_position(arg).column =
                    block.safe_get_by_position(arg).column;

        //        if (auto * res_low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(res.type.get()))
        //        {
        //            const auto * low_cardinality_column = findLowCardinalityArgument(block, args);
        //            bool can_be_executed_on_default_arguments = canBeExecutedOnDefaultArguments();
        //            bool use_cache = low_cardinality_result_cache && can_be_executed_on_default_arguments
        //                             && low_cardinality_column && low_cardinality_column->isSharedDictionary();
        //            PreparedFunctionLowCardinalityResultCache::DictionaryKey key;
        //
        //            if (use_cache)
        //            {
        //                const auto & dictionary = low_cardinality_column->getDictionary();
        //                key = {dictionary.getHash(), dictionary.size()};
        //
        //                auto cached_values = low_cardinality_result_cache->get(key);
        //                if (cached_values)
        //                {
        //                    auto indexes = cached_values->index_mapping->index(low_cardinality_column->getIndexes(), 0);
        //                    res.column = ColumnLowCardinality::create(cached_values->function_result, indexes, true);
        //                    return;
        //                }
        //            }
        //
        //            block_without_low_cardinality.safe_get_by_position(result).type = res_low_cardinality_type->getDictionaryType();
        //            ColumnPtr indexes = replaceLowCardinalityColumnsByNestedAndGetDictionaryIndexes(
        //                    block_without_low_cardinality, args, can_be_executed_on_default_arguments, input_rows_count);
        //
        //            executeWithoutLowCardinalityColumns(block_without_low_cardinality, args, result, block_without_low_cardinality.rows(), dry_run);
        //
        //            auto keys = block_without_low_cardinality.safe_get_by_position(result).column->convert_to_full_column_if_const();
        //
        //            auto res_mut_dictionary = DataTypeLowCardinality::createColumnUnique(*res_low_cardinality_type->getDictionaryType());
        //            ColumnPtr res_indexes = res_mut_dictionary->uniqueInsertRangeFrom(*keys, 0, keys->size());
        //            ColumnUniquePtr res_dictionary = std::move(res_mut_dictionary);
        //
        //            if (indexes)
        //            {
        //                if (use_cache)
        //                {
        //                    auto cache_values = std::make_shared<PreparedFunctionLowCardinalityResultCache::CachedValues>();
        //                    cache_values->dictionary_holder = low_cardinality_column->getDictionaryPtr();
        //                    cache_values->function_result = res_dictionary;
        //                    cache_values->index_mapping = res_indexes;
        //
        //                    cache_values = low_cardinality_result_cache->getOrSet(key, cache_values);
        //                    res_dictionary = cache_values->function_result;
        //                    res_indexes = cache_values->index_mapping;
        //                }
        //
        //                res.column = ColumnLowCardinality::create(res_dictionary, res_indexes->index(*indexes, 0), use_cache);
        //            }
        //            else
        //            {
        //                res.column = ColumnLowCardinality::create(res_dictionary, res_indexes);
        //            }
        //        }
        //        else
        {
            //            convertLowCardinalityColumnsToFull(block_without_low_cardinality, args);
            RETURN_IF_ERROR(executeWithoutLowCardinalityColumns(block_without_low_cardinality, args,
                                                                result, input_rows_count, dry_run));
            res.column = block_without_low_cardinality.safe_get_by_position(result).column;
        }
    } else
        executeWithoutLowCardinalityColumns(block, args, result, input_rows_count, dry_run);
    return Status::OK();
}

void FunctionBuilderImpl::checkNumberOfArguments(size_t number_of_arguments) const {
    if (isVariadic()) return;

    size_t expected_number_of_arguments = getNumberOfArguments();

    CHECK_EQ(number_of_arguments, expected_number_of_arguments) << fmt::format(
            "Number of arguments for function {} doesn't match: passed {} , should be {}",
            get_name(), number_of_arguments, expected_number_of_arguments);
}

DataTypePtr FunctionBuilderImpl::getReturnTypeWithoutLowCardinality(
        const ColumnsWithTypeAndName& arguments) const {
    checkNumberOfArguments(arguments.size());

    if (!arguments.empty() && useDefaultImplementationForNulls()) {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant) {
            return make_nullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable) {
            ColumnNumbers numbers(arguments.size());
            for (size_t i = 0; i < arguments.size(); i++) {
                numbers[i] = i;
            }
            Block nested_block = createBlockWithNestedColumns(Block(arguments), numbers);
            auto return_type = getReturnTypeImpl(
                    ColumnsWithTypeAndName(nested_block.begin(), nested_block.end()));
            return make_nullable(return_type);
        }
    }

    return getReturnTypeImpl(arguments);
}

#if USE_EMBEDDED_COMPILER

static std::optional<DataTypes> removeNullables(const DataTypes& types) {
    for (const auto& type : types) {
        if (!typeid_cast<const DataTypeNullable*>(type.get())) continue;
        DataTypes filtered;
        for (const auto& sub_type : types) filtered.emplace_back(removeNullable(sub_type));
        return filtered;
    }
    return {};
}

bool IFunction::isCompilable(const DataTypes& arguments) const {
    if (useDefaultImplementationForNulls())
        if (auto denulled = removeNullables(arguments)) return isCompilableImpl(*denulled);
    return isCompilableImpl(arguments);
}

llvm::Value* IFunction::compile(llvm::IRBuilderBase& builder, const DataTypes& arguments,
                                ValuePlaceholders values) const {
    if (useDefaultImplementationForNulls()) {
        if (auto denulled = removeNullables(arguments)) {
            /// FIXME: when only one column is nullable, this can actually be slower than the non-jitted version
            ///        because this involves copying the null map while `wrapInNullable` reuses it.
            auto& b = static_cast<llvm::IRBuilder<>&>(builder);
            auto* fail = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "",
                                                  b.GetInsertBlock()->getParent());
            auto* join = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "",
                                                  b.GetInsertBlock()->getParent());
            auto* zero = llvm::Constant::getNullValue(
                    toNativeType(b, make_nullable(getReturnTypeImpl(*denulled))));
            for (size_t i = 0; i < arguments.size(); i++) {
                if (!arguments[i]->is_nullable()) continue;
                /// Would be nice to evaluate all this lazily, but that'd change semantics: if only unevaluated
                /// arguments happen to contain NULLs, the return value would not be NULL, though it should be.
                auto* value = values[i]();
                auto* ok = llvm::BasicBlock::Create(b.GetInsertBlock()->getContext(), "",
                                                    b.GetInsertBlock()->getParent());
                b.CreateCondBr(b.CreateExtractValue(value, {1}), fail, ok);
                b.SetInsertPoint(ok);
                values[i] = [value = b.CreateExtractValue(value, {0})]() { return value; };
            }
            auto* result = b.CreateInsertValue(
                    zero, compileImpl(builder, *denulled, std::move(values)), {0});
            auto* result_block = b.GetInsertBlock();
            b.CreateBr(join);
            b.SetInsertPoint(fail);
            auto* null = b.CreateInsertValue(zero, b.getTrue(), {1});
            b.CreateBr(join);
            b.SetInsertPoint(join);
            auto* phi = b.CreatePHI(result->getType(), 2);
            phi->addIncoming(result, result_block);
            phi->addIncoming(null, fail);
            return phi;
        }
    }
    return compileImpl(builder, arguments, std::move(values));
}

#endif

DataTypePtr FunctionBuilderImpl::getReturnType(const ColumnsWithTypeAndName& arguments) const {
    if (useDefaultImplementationForLowCardinalityColumns()) {
        //        bool has_low_cardinality = false;
        //        size_t num_full_low_cardinality_columns = 0;
        size_t num_full_ordinary_columns = 0;

        ColumnsWithTypeAndName args_without_low_cardinality(arguments);

        for (ColumnWithTypeAndName& arg : args_without_low_cardinality) {
            bool is_const = arg.column && is_column_const(*arg.column);
            if (is_const)
                arg.column = assert_cast<const ColumnConst&>(*arg.column).remove_low_cardinality();

            //            if (auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(arg.type.get()))
            //            {
            //                arg.type = low_cardinality_type->getDictionaryType();
            //                has_low_cardinality = true;
            //
            //                if (!is_const)
            //                    ++num_full_low_cardinality_columns;
            //            }
            //            else
            if (!is_const) ++num_full_ordinary_columns;
        }

        //        for (auto & arg : args_without_low_cardinality)
        //        {
        //            arg.column = recursiveRemoveLowCardinality(arg.column);
        //            arg.type = recursiveRemoveLowCardinality(arg.type);
        //        }

        auto type_without_low_cardinality =
                getReturnTypeWithoutLowCardinality(args_without_low_cardinality);

        //        if (canBeExecutedOnLowCardinalityDictionary() && has_low_cardinality
        //            && num_full_low_cardinality_columns <= 1 && num_full_ordinary_columns == 0
        //            && type_without_low_cardinality->canBeInsideLowCardinality())
        //            return std::make_shared<DataTypeLowCardinality>(type_without_low_cardinality);
        //        else
        return type_without_low_cardinality;
    }

    return getReturnTypeWithoutLowCardinality(arguments);
}
} // namespace doris::vectorized
