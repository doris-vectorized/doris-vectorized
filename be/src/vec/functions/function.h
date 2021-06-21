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

#include <memory>

//#include "config_core.h"
#include "common/status.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/names.h"
#include "vec/data_types/data_type.h"

namespace llvm {
class LLVMContext;
class Value;
class IRBuilderBase;
} // namespace llvm

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

class Field;

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each block.
///  * Provide const interface for IFunctionBase (later).
class IPreparedFunction {
public:
    virtual ~IPreparedFunction() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                           size_t input_rows_count, bool dry_run) = 0;
};

using PreparedFunctionPtr = std::shared_ptr<IPreparedFunction>;

/// Cache for functions result if it was executed on low cardinality column.
class PreparedFunctionLowCardinalityResultCache;
using PreparedFunctionLowCardinalityResultCachePtr =
        std::shared_ptr<PreparedFunctionLowCardinalityResultCache>;

class PreparedFunctionImpl : public IPreparedFunction {
public:
    Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                   size_t input_rows_count, bool dry_run = false) final;

    /// Create cache which will be used to store result of function executed on LowCardinality column.
    /// Only for default LowCardinality implementation.
    /// Cannot be called concurrently for the same object.
    void createLowCardinalityResultCache(size_t cache_size);

protected:
    virtual Status executeImplDryRun(Block& block, const ColumnNumbers& arguments, size_t result,
                                     size_t input_rows_count) {
        return executeImpl(block, arguments, result, input_rows_count);
    }
    virtual Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                               size_t input_rows_count) = 0;

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool useDefaultImplementationForConstants() const { return false; }

    /** If function arguments has single low cardinality column and all other arguments are constants, call function on nested column.
      * Otherwise, convert all low cardinality columns to ordinary columns.
      * Returns ColumnLowCardinality if at least one argument is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /** Some arguments could remain constant during this implementation.
      */
    virtual ColumnNumbers get_argumentsThatAreAlwaysConstant() const { return {}; }

    /** True if function can be called on default arguments (include Nullable's) and won't throw.
      * Counterexample: modulo(0, 0)
      */
    virtual bool canBeExecutedOnDefaultArguments() const { return true; }

private:
    Status defaultImplementationForNulls(Block& block, const ColumnNumbers& args, size_t result,
                                         size_t input_rows_count, bool dry_run, bool* executed);
    Status defaultImplementationForConstantArguments(Block& block, const ColumnNumbers& args,
                                                     size_t result, size_t input_rows_count,
                                                     bool dry_run, bool* executed);
    Status executeWithoutLowCardinalityColumns(Block& block, const ColumnNumbers& arguments,
                                               size_t result, size_t input_rows_count,
                                               bool dry_run);

    /// Cache is created by function createLowCardinalityResultCache()
    PreparedFunctionLowCardinalityResultCachePtr low_cardinality_result_cache;
};

using ValuePlaceholders = std::vector<std::function<llvm::Value*()>>;

/// Function with known arguments and return type.
class IFunctionBase {
public:
    virtual ~IFunctionBase() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual const DataTypes& get_argument_types() const = 0;
    virtual const DataTypePtr& get_return_type() const = 0;

    /// Do preparations and return executable.
    /// sample_block should contain data types of arguments and values of constants, if relevant.
    virtual PreparedFunctionPtr prepare(const Block& sample_block, const ColumnNumbers& arguments,
                                        size_t result) const = 0;

    /// TODO: make const
    virtual Status execute(Block& block, const ColumnNumbers& arguments, size_t result,
                           size_t input_rows_count, bool dry_run = false) {
        return prepare(block, arguments, result)
                ->execute(block, arguments, result, input_rows_count, dry_run);
    }

#if USE_EMBEDDED_COMPILER

    virtual bool isCompilable() const { return false; }

    /** Produce LLVM IR code that operates on scalar values. See `toNativeType` in DataTypes/Native.h
      * for supported value types and how they map to LLVM types.
      *
      * NOTE: the builder is actually guaranteed to be exactly `llvm::IRBuilder<>`, so you may safely
      *       downcast it to that type. This method is specified with `IRBuilderBase` because forward-declaring
      *       templates with default arguments is impossible and including LLVM in such a generic header
      *       as this one is a major pain.
      */
    virtual llvm::Value* compile(llvm::IRBuilderBase& /*builder*/,
                                 ValuePlaceholders /*values*/) const {
        throw Exception(get_name() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif

    virtual bool is_stateful() const { return false; }

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** Some functions like ignore(...) or toTypeName(...) always return constant result which doesn't depend on arguments.
      * In this case we can calculate result and assume that it's constant in stream header.
      * There is no need to implement function if it has zero arguments.
      * Must return ColumnConst with single row or nullptr.
      */
    virtual ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(
            const Block& /*block*/, const ColumnNumbers& /*arguments*/) const {
        return nullptr;
    }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisible by two;
      *  concat(x, 'const');
      *  concat(x, 'const', y) where const contain at least one non-numeric character;
      *  concat with FixedString
      *  dictGet... functions takes name of dictionary as its argument,
      *   and some dictionaries could be explicitly defined as injective.
      *
      * It could be used, for example, to remove useless function applications from GROUP BY.
      *
      * Sometimes, function is not really injective, but considered as injective, for purpose of query optimization.
      * For example, to_string function is not injective for Float64 data type,
      *  as it returns 'nan' for many different representation of NaNs.
      * But we assume, that it is injective. This could be documented as implementation-specific behaviour.
      *
      * sample_block should contain data types of arguments and values of constants, if relevant.
      */
    virtual bool isInjective(const Block& /*sample_block*/) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */

    virtual bool isDeterministic() const = 0;

    virtual bool isDeterministicInScopeOfQuery() const = 0;

    /** Lets you know if the function is monotonic in a range of values.
      * This is used to work with the index in a sorted chunk of data.
      * And allows to use the index not only when it is written, for example `date >= const`, but also, for example, `toMonth(date) >= 11`.
      * All this is considered only for functions of one argument.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    /// The property of monotonicity for a certain range.
    struct Monotonicity {
        bool is_monotonic = false; /// Is the function monotonous (nondecreasing or nonincreasing).
        bool is_positive =
                true; /// true if the function is nondecreasing, false, if notincreasing. If is_monotonic = false, then it does not matter.
        bool is_always_monotonic =
                false; /// Is true if function is monotonic on the whole input range I

        Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true,
                     bool is_always_monotonic_ = false)
                : is_monotonic(is_monotonic_),
                  is_positive(is_positive_),
                  is_always_monotonic(is_always_monotonic_) {}
    };

    /** Get information about monotonicity on a range of values. Call only if hasInformationAboutMonotonicity.
      * NULL can be passed as one of the arguments. This means that the corresponding range is unlimited on the left or on the right.
      */
    virtual Monotonicity getMonotonicityForRange(const IDataType& /*type*/, const Field& /*left*/,
                                                 const Field& /*right*/) const {
        LOG(FATAL) << fmt::format("Function {} has no information about its monotonicity.", get_name());
        return Monotonicity{};
    }
};

using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/// Creates IFunctionBase from argument types list.
class IFunctionBuilder {
public:
    virtual ~IFunctionBuilder() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    /// See the comment for the same method in IFunctionBase
    virtual bool isDeterministic() const = 0;

    virtual bool isDeterministicInScopeOfQuery() const = 0;

    /// Override and return true if function needs to depend on the state of the data.
    virtual bool is_stateful() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const = 0;

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// Throw if number of arguments is incorrect. Default implementation will check only in non-variadic case.
    virtual void checkNumberOfArguments(size_t number_of_arguments) const = 0;

    /// Check arguments and return IFunctionBase.
    virtual FunctionBasePtr build(const ColumnsWithTypeAndName& arguments) const = 0;

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual void getLambdaArgumentTypes(DataTypes& arguments) const = 0;

    /// Returns indexes of arguments, that must be ColumnConst
    virtual ColumnNumbers get_argumentsThatAreAlwaysConstant() const = 0;
    /// Returns indexes if arguments, that can be Nullable without making result of function Nullable
    /// (for functions like is_null(x))
    virtual ColumnNumbers get_argumentsThatDontImplyNullableReturnType(
            size_t number_of_arguments) const = 0;
};

using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

class FunctionBuilderImpl : public IFunctionBuilder {
public:
    FunctionBasePtr build(const ColumnsWithTypeAndName& arguments) const final {
        return buildImpl(arguments, get_return_type(arguments));
    }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool is_stateful() const override { return false; }
    bool isVariadic() const override { return false; }

    /// Default implementation. Will check only in non-variadic case.
    void checkNumberOfArguments(size_t number_of_arguments) const override;

    DataTypePtr get_return_type(const ColumnsWithTypeAndName& arguments) const;

    void getLambdaArgumentTypes(DataTypes& arguments) const override {
        checkNumberOfArguments(arguments.size());
        getLambdaArgumentTypesImpl(arguments);
    }

    ColumnNumbers get_argumentsThatAreAlwaysConstant() const override { return {}; }
    ColumnNumbers get_argumentsThatDontImplyNullableReturnType(
            size_t /*number_of_arguments*/) const override {
        return {};
    }

protected:
    /// Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    virtual DataTypePtr get_return_typeImpl(const ColumnsWithTypeAndName& arguments) const {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;

        return get_return_typeImpl(data_types);
    }

    virtual DataTypePtr get_return_typeImpl(const DataTypes& /*arguments*/) const {
        LOG(FATAL) << fmt::format("getReturnType is not implemented for {}", get_name());
    }

    /** If useDefaultImplementationForNulls() is true, than change arguments for get_return_type() and buildImpl():
      *  if some of arguments are Nullable(Nothing) then don't call get_return_type(), call buildImpl() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for get_return_type() function
      *   - wrap get_return_type() result in Nullable type and pass to buildImpl
      *
      * Otherwise build returns buildImpl(arguments, get_return_type(arguments));
      */
    virtual bool useDefaultImplementationForNulls() const { return true; }

    /** If useDefaultImplementationForNulls() is true, than change arguments for get_return_type() and buildImpl().
      * If function arguments has low cardinality types, convert them to ordinary types.
      * get_return_type returns ColumnLowCardinality if at least one argument type is ColumnLowCardinality.
      */
    virtual bool useDefaultImplementationForLowCardinalityColumns() const { return true; }

    /// If it isn't, will convert all ColumnLowCardinality arguments to full columns.
    virtual bool canBeExecutedOnLowCardinalityDictionary() const { return true; }

    virtual FunctionBasePtr buildImpl(const ColumnsWithTypeAndName& arguments,
                                      const DataTypePtr& return_type) const = 0;

    virtual void getLambdaArgumentTypesImpl(DataTypes& /*arguments*/) const {
        LOG(FATAL) << fmt::format("Function {} can't have lambda-expressions as arguments", get_name());
    }

private:
    DataTypePtr get_return_typeWithoutLowCardinality(const ColumnsWithTypeAndName& arguments) const;
};

/// Previous function interface.
class IFunction : public std::enable_shared_from_this<IFunction>,
                  public FunctionBuilderImpl,
                  public IFunctionBase,
                  public PreparedFunctionImpl {
public:
    String get_name() const override = 0;

    bool is_stateful() const override { return false; }

    /// TODO: make const
    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) override = 0;

    /// Override this functions to change default implementation behavior. See details in IMyFunction.
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    ColumnNumbers get_argumentsThatAreAlwaysConstant() const override { return {}; }
    bool canBeExecutedOnDefaultArguments() const override { return true; }
    bool canBeExecutedOnLowCardinalityDictionary() const override {
        return isDeterministicInScopeOfQuery();
    }
    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    using PreparedFunctionImpl::execute;
    using PreparedFunctionImpl::executeImplDryRun;
    using FunctionBuilderImpl::get_return_typeImpl;
    using FunctionBuilderImpl::getLambdaArgumentTypesImpl;
    using FunctionBuilderImpl::get_return_type;

    PreparedFunctionPtr prepare(const Block& /*sample_block*/, const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const final {
        LOG(FATAL) << "prepare is not implemented for IFunction";
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const final {
        throw Exception("isCompilable without explicit types is not implemented for IFunction",
                        ErrorCodes::NOT_IMPLEMENTED);
    }

    llvm::Value* compile(llvm::IRBuilderBase& /*builder*/,
                         ValuePlaceholders /*values*/) const final {
        throw Exception("compile without explicit types is not implemented for IFunction",
                        ErrorCodes::NOT_IMPLEMENTED);
    }

#endif

    const DataTypes& get_argument_types() const final {
        LOG(FATAL) << "get_argument_types is not implemented for IFunction";
    }

    const DataTypePtr& get_return_type() const final {
        LOG(FATAL) << "get_return_type is not implemented for IFunction";
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable(const DataTypes& arguments) const;

    llvm::Value* compile(llvm::IRBuilderBase&, const DataTypes& arguments,
                         ValuePlaceholders values) const;

#endif

protected:
#if USE_EMBEDDED_COMPILER

    virtual bool isCompilableImpl(const DataTypes&) const { return false; }

    virtual llvm::Value* compileImpl(llvm::IRBuilderBase&, const DataTypes&,
                                     ValuePlaceholders) const {
        throw Exception(get_name() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
    }

#endif

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName& /*arguments*/,
                              const DataTypePtr& /*return_type*/) const final {
        LOG(FATAL) << "buildImpl is not implemented for IFunction";
        return {};
    }
};

/// Wrappers over IFunction.

class DefaultExecutable final : public PreparedFunctionImpl {
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    String get_name() const override { return function->get_name(); }

protected:
    Status executeImpl(Block& block, const ColumnNumbers& arguments, size_t result,
                       size_t input_rows_count) final {
        return function->executeImpl(block, arguments, result, input_rows_count);
    }
    Status executeImplDryRun(Block& block, const ColumnNumbers& arguments, size_t result,
                             size_t input_rows_count) final {
        return function->executeImplDryRun(block, arguments, result, input_rows_count);
    }
    bool useDefaultImplementationForNulls() const final {
        return function->useDefaultImplementationForNulls();
    }
    bool useDefaultImplementationForConstants() const final {
        return function->useDefaultImplementationForConstants();
    }
    bool useDefaultImplementationForLowCardinalityColumns() const final {
        return function->useDefaultImplementationForLowCardinalityColumns();
    }
    ColumnNumbers get_argumentsThatAreAlwaysConstant() const final {
        return function->get_argumentsThatAreAlwaysConstant();
    }
    bool canBeExecutedOnDefaultArguments() const override {
        return function->canBeExecutedOnDefaultArguments();
    }

private:
    std::shared_ptr<IFunction> function;
};

class DefaultFunction final : public IFunctionBase {
public:
    DefaultFunction(std::shared_ptr<IFunction> function_, DataTypes arguments_,
                    DataTypePtr return_type_)
            : function(std::move(function_)),
              arguments(std::move(arguments_)),
              return_type(std::move(return_type_)) {}

    String get_name() const override { return function->get_name(); }

    const DataTypes& get_argument_types() const override { return arguments; }
    const DataTypePtr& get_return_type() const override { return return_type; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override { return function->isCompilable(arguments); }

    llvm::Value* compile(llvm::IRBuilderBase& builder, ValuePlaceholders values) const override {
        return function->compile(builder, arguments, std::move(values));
    }

#endif

    PreparedFunctionPtr prepare(const Block& /*sample_block*/, const ColumnNumbers& /*arguments*/,
                                size_t /*result*/) const override {
        return std::make_shared<DefaultExecutable>(function);
    }

    bool isSuitableForConstantFolding() const override {
        return function->isSuitableForConstantFolding();
    }
    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(
            const Block& block, const ColumnNumbers& arguments_) const override {
        return function->getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments_);
    }

    bool isInjective(const Block& sample_block) override {
        return function->isInjective(sample_block);
    }

    bool isDeterministic() const override { return function->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override {
        return function->isDeterministicInScopeOfQuery();
    }

    bool hasInformationAboutMonotonicity() const override {
        return function->hasInformationAboutMonotonicity();
    }

    IFunctionBase::Monotonicity getMonotonicityForRange(const IDataType& type, const Field& left,
                                                        const Field& right) const override {
        return function->getMonotonicityForRange(type, left, right);
    }

private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultFunctionBuilder : public FunctionBuilderImpl {
public:
    explicit DefaultFunctionBuilder(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    void checkNumberOfArguments(size_t number_of_arguments) const override {
        return function->checkNumberOfArguments(number_of_arguments);
    }

    bool isDeterministic() const override { return function->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const override {
        return function->isDeterministicInScopeOfQuery();
    }

    String get_name() const override { return function->get_name(); }
    bool is_stateful() const override { return function->is_stateful(); }
    bool isVariadic() const override { return function->isVariadic(); }
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }

    ColumnNumbers get_argumentsThatAreAlwaysConstant() const override {
        return function->get_argumentsThatAreAlwaysConstant();
    }
    ColumnNumbers get_argumentsThatDontImplyNullableReturnType(
            size_t number_of_arguments) const override {
        return function->get_argumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

protected:
    DataTypePtr get_return_typeImpl(const DataTypes& arguments) const override {
        return function->get_return_typeImpl(arguments);
    }
    DataTypePtr get_return_typeImpl(const ColumnsWithTypeAndName& arguments) const override {
        return function->get_return_typeImpl(arguments);
    }

    bool useDefaultImplementationForNulls() const override {
        return function->useDefaultImplementationForNulls();
    }
    bool useDefaultImplementationForLowCardinalityColumns() const override {
        return function->useDefaultImplementationForLowCardinalityColumns();
    }
    bool canBeExecutedOnLowCardinalityDictionary() const override {
        return function->canBeExecutedOnLowCardinalityDictionary();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName& arguments,
                              const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) data_types[i] = arguments[i].type;
        return std::make_shared<DefaultFunction>(function, data_types, return_type);
    }

    void getLambdaArgumentTypesImpl(DataTypes& arguments) const override {
        return function->getLambdaArgumentTypesImpl(arguments);
    }

private:
    std::shared_ptr<IFunction> function;
};

using FunctionPtr = std::shared_ptr<IFunction>;

/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns in blocks.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrapInNullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                         size_t result, size_t input_rows_count);

} // namespace doris::vectorized
