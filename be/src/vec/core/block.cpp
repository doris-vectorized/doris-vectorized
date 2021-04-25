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

#include "vec/core/block.h"

#include <iterator>
#include <memory>

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/exception.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_types_decimal.h"
#include "vec/data_types/data_types_number.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int POSITION_OUT_OF_BOUND;
extern const int NOT_FOUND_COLUMN_IN_BLOCK;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
extern const int BLOCKS_HAVE_DIFFERENT_STRUCTURE;
extern const int UNKNOWN_TYPE;
} // namespace ErrorCodes

inline DataTypePtr get_data_type(const PColumn& pcolumn) {
    switch (pcolumn.type()) {
    case PColumn::UINT8: {
        return std::make_shared<DataTypeUInt8>();
    }
    case PColumn::UINT16: {
        return std::make_shared<DataTypeUInt16>();
    }
    case PColumn::UINT32: {
        return std::make_shared<DataTypeUInt32>();
    }
    case PColumn::UINT64: {
        return std::make_shared<DataTypeUInt64>();
    }
    case PColumn::UINT128: {
        return std::make_shared<DataTypeUInt128>();
    }
    case PColumn::INT8: {
        return std::make_shared<DataTypeInt8>();
    }
    case PColumn::INT16: {
        return std::make_shared<DataTypeInt16>();
    }
    case PColumn::INT32: {
        return std::make_shared<DataTypeInt32>();
    }
    case PColumn::INT64: {
        return std::make_shared<DataTypeInt64>();
    }
    case PColumn::INT128: {
        return std::make_shared<DataTypeInt128>();
    }
    case PColumn::FLOAT32: {
        return std::make_shared<DataTypeFloat32>();
    }
    case PColumn::FLOAT64: {
        return std::make_shared<DataTypeFloat64>();
    }
    case PColumn::STRING: {
        return std::make_shared<DataTypeString>();
    }
    case PColumn::DECIMAL32: {
        return std::make_shared<DataTypeDecimal<Decimal32>>(pcolumn.decimal_param().precision(),
                                                            pcolumn.decimal_param().scale());
    }
    case PColumn::DECIMAL64: {
        return std::make_shared<DataTypeDecimal<Decimal64>>(pcolumn.decimal_param().precision(),
                                                            pcolumn.decimal_param().scale());
    }
    case PColumn::DECIMAL128: {
        return std::make_shared<DataTypeDecimal<Decimal128>>(pcolumn.decimal_param().precision(),
                                                             pcolumn.decimal_param().scale());
    }
    default: {
        throw Exception("Unknown data type: " + std::to_string(pcolumn.type()) +
                                ", data type name: " + PColumn::DataType_Name(pcolumn.type()),
                        ErrorCodes::UNKNOWN_TYPE);
        break;
    }
    }
}

PColumn::DataType get_pdata_type(DataTypePtr data_type) {
    switch (data_type->getTypeId()) {
    case TypeIndex::UInt8:
        return PColumn::UINT8;
    case TypeIndex::UInt16:
        return PColumn::UINT16;
    case TypeIndex::UInt32:
        return PColumn::UINT32;
    case TypeIndex::UInt64:
        return PColumn::UINT64;
    case TypeIndex::UInt128:
        return PColumn::UINT128;
    case TypeIndex::Int8:
        return PColumn::INT8;
    case TypeIndex::Int16:
        return PColumn::INT16;
    case TypeIndex::Int32:
        return PColumn::INT32;
    case TypeIndex::Int64:
        return PColumn::INT64;
    case TypeIndex::Int128:
        return PColumn::INT128;
    case TypeIndex::Float32:
        return PColumn::FLOAT32;
    case TypeIndex::Float64:
        return PColumn::FLOAT64;
    case TypeIndex::Decimal32:
        return PColumn::DECIMAL32;
    case TypeIndex::Decimal64:
        return PColumn::DECIMAL64;
    case TypeIndex::Decimal128:
        return PColumn::DECIMAL128;
    case TypeIndex::String:
        return PColumn::STRING;
    default:
        return PColumn::UNKNOWN;
    }
}

Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il} {
    initializeIndexByName();
}

Block::Block(const ColumnsWithTypeAndName& data_) : data{data_} {
    initializeIndexByName();
}

Block::Block(const PBlock& pblock) {
    for (const auto& pcolumn : pblock.columns()) {
        DataTypePtr type = get_data_type(pcolumn);
        MutableColumnPtr data_column;
        if (pcolumn.is_null_size() > 0) {
            data_column =
                    ColumnNullable::create(std::move(type->createColumn()), ColumnUInt8::create());
            type = makeNullable(type);
        } else {
            data_column = type->createColumn();
        }
        type->deserialize(pcolumn, data_column.get());
        data.emplace_back(data_column->getPtr(), type, pcolumn.name());
    }
    initializeIndexByName();
}

void Block::initializeIndexByName() {
    for (size_t i = 0, size = data.size(); i < size; ++i) index_by_name[data[i].name] = i;
}

void Block::insert(size_t position, const ColumnWithTypeAndName& elem) {
    if (position > data.size())
        throw Exception("Position out of bound in Block::insert(), max position = " +
                                std::to_string(data.size()),
                        ErrorCodes::POSITION_OUT_OF_BOUND);

    for (auto& name_pos : index_by_name)
        if (name_pos.second >= position) ++name_pos.second;

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, elem);
}

void Block::insert(size_t position, ColumnWithTypeAndName&& elem) {
    if (position > data.size())
        throw Exception("Position out of bound in Block::insert(), max position = " +
                                std::to_string(data.size()),
                        ErrorCodes::POSITION_OUT_OF_BOUND);

    for (auto& name_pos : index_by_name)
        if (name_pos.second >= position) ++name_pos.second;

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, std::move(elem));
}

void Block::insert(const ColumnWithTypeAndName& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName&& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(std::move(elem));
}

void Block::insertUnique(const ColumnWithTypeAndName& elem) {
    if (index_by_name.end() == index_by_name.find(elem.name)) insert(elem);
}

void Block::insertUnique(ColumnWithTypeAndName&& elem) {
    if (index_by_name.end() == index_by_name.find(elem.name)) insert(std::move(elem));
}

void Block::erase(const std::set<size_t>& positions) {
    for (auto it = positions.rbegin(); it != positions.rend(); ++it) erase(*it);
}

void Block::erase(size_t position) {
    if (data.empty()) throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position out of bound in Block::erase(), max position = " +
                                std::to_string(data.size() - 1),
                        ErrorCodes::POSITION_OUT_OF_BOUND);

    eraseImpl(position);
}

void Block::eraseImpl(size_t position) {
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second == position)
            index_by_name.erase(it++);
        else {
            if (it->second > position) --it->second;
            ++it;
        }
    }
}

void Block::erase(const String& name) {
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end())
        throw Exception("No such name in Block::erase(): '" + name + "'",
                        ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    eraseImpl(index_it->second);
}

ColumnWithTypeAndName& Block::safeGetByPosition(size_t position) {
    if (data.empty()) throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position " + std::to_string(position) +
                                " is out of bound in Block::safeGetByPosition(), max position = " +
                                std::to_string(data.size() - 1) +
                                ", there are columns: " + dumpNames(),
                        ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}

const ColumnWithTypeAndName& Block::safeGetByPosition(size_t position) const {
    if (data.empty()) throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception(
                "Position " + std::to_string(position) +
                        " is out of bound in Block::safeGetByPosition(), max position = " +
                        std::to_string(data.size() - 1)
                        // + ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);
                        + ", there are columns: ",
                ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}

ColumnWithTypeAndName& Block::getByName(const std::string& name) {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception(
                "Not found column " + name + " in block. There are only columns: " + dumpNames(),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return data[it->second];
}

const ColumnWithTypeAndName& Block::getByName(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception(
                "Not found column " + name + " in block. There are only columns: " + dumpNames(),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return data[it->second];
}

bool Block::has(const std::string& name) const {
    return index_by_name.end() != index_by_name.find(name);
}

size_t Block::getPositionByName(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception(
                "Not found column " + name + " in block. There are only columns: " + dumpNames(),
                ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return it->second;
}

void Block::checkNumberOfRows(bool allow_null_columns) const {
    ssize_t rows = -1;
    for (const auto& elem : data) {
        if (!elem.column && allow_null_columns) continue;

        if (!elem.column)
            throw Exception(
                    "Column " + elem.name + " in block is nullptr, in method checkNumberOfRows.",
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        ssize_t size = elem.column->size();

        if (rows == -1)
            rows = size;
        else if (rows != size)
            throw Exception("Sizes of columns doesn't match: " + data.front().name + ": " +
                                    std::to_string(rows) + ", " + elem.name + ": " +
                                    std::to_string(size),
                            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}

size_t Block::rows() const {
    for (const auto& elem : data)
        if (elem.column) return elem.column->size();

    return 0;
}

void Block::set_num_rows(int length) {
    if (rows() > length) {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->cut(0, length);
            }
        }
    }
}

size_t Block::bytes() const {
    size_t res = 0;
    for (const auto& elem : data) res += elem.column->byteSize();

    return res;
}

size_t Block::allocatedBytes() const {
    size_t res = 0;
    for (const auto& elem : data) res += elem.column->allocatedBytes();

    return res;
}

std::string Block::dumpNames() const {
    // WriteBufferFromOwnString out;
    std::stringstream out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) out << ", ";
        out << it->name;
    }
    return out.str();
}

std::string Block::dumpData() const {
    // WriteBufferFromOwnString out;
    std::stringstream out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) out << ", ";
        out << it->name;
    }
    return out.str();
}

std::string Block::dumpStructure() const {
    // WriteBufferFromOwnString out;
    std::stringstream out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) out << ", ";
        out << it->dumpStructure();
    }
    return out.str();
}

Block Block::cloneEmpty() const {
    Block res;

    for (const auto& elem : data) res.insert(elem.cloneEmpty());

    return res;
}

MutableColumns Block::cloneEmptyColumns() const {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? data[i].column->cloneEmpty() : data[i].type->createColumn();
    return columns;
}

Columns Block::getColumns() const {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) columns[i] = data[i].column;
    return columns;
}

MutableColumns Block::mutateColumns() {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? (*std::move(data[i].column)).mutate()
                                    : data[i].type->createColumn();
    return columns;
}

void Block::setColumns(MutableColumns&& columns) {
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) data[i].column = std::move(columns[i]);
}

void Block::setColumns(const Columns& columns) {
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) data[i].column = columns[i];
}

Block Block::cloneWithColumns(MutableColumns&& columns) const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        res.insert({std::move(columns[i]), data[i].type, data[i].name});

    return res;
}

Block Block::cloneWithColumns(const Columns& columns) const {
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size())
        throw Exception("Cannot clone block with columns because block has " +
                                std::to_string(num_columns) +
                                " columns, "
                                "but " +
                                std::to_string(columns.size()) + " columns given.",
                        ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < num_columns; ++i) res.insert({columns[i], data[i].type, data[i].name});

    return res;
}

Block Block::cloneWithoutColumns() const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) res.insert({nullptr, data[i].type, data[i].name});

    return res;
}

Block Block::sortColumns() const {
    Block sorted_block;

    for (const auto& name : index_by_name) sorted_block.insert(data[name.second]);

    return sorted_block;
}

const ColumnsWithTypeAndName& Block::getColumnsWithTypeAndName() const {
    return data;
}

NamesAndTypesList Block::getNamesAndTypesList() const {
    NamesAndTypesList res;

    for (const auto& elem : data) res.emplace_back(elem.name, elem.type);

    return res;
}

Names Block::getNames() const {
    Names res;
    res.reserve(columns());

    for (const auto& elem : data) res.push_back(elem.name);

    return res;
}

DataTypes Block::getDataTypes() const {
    DataTypes res;
    res.reserve(columns());

    for (const auto& elem : data) res.push_back(elem.type);

    return res;
}

// template <typename ReturnType>
// static ReturnType checkBlockStructure(const Block & lhs, const Block & rhs, const std::string & context_description)
// {
//     auto on_error = [](const std::string & message [[maybe_unused]], int code [[maybe_unused]])
//     {
//         if constexpr (std::is_same_v<ReturnType, void>)
//             throw Exception(message, code);
//         else
//             return false;
//     };

//     size_t columns = rhs.columns();
//     if (lhs.columns() != columns)
//         return on_error("Block structure mismatch in " + context_description + " stream: different number of columns:\n"
//             + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);

//     for (size_t i = 0; i < columns; ++i)
//     {
//         const auto & expected = rhs.getByPosition(i);
//         const auto & actual = lhs.getByPosition(i);

//         if (actual.name != expected.name)
//             return on_error("Block structure mismatch in " + context_description + " stream: different names of columns:\n"
//                 + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);

//         if (!actual.type->equals(*expected.type))
//             return on_error("Block structure mismatch in " + context_description + " stream: different types:\n"
//                 + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);

//         if (!actual.column || !expected.column)
//             continue;

//         if (actual.column->getName() != expected.column->getName())
//             return on_error("Block structure mismatch in " + context_description + " stream: different columns:\n"
//                 + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);

// if (isColumnConst(*actual.column) && isColumnConst(*expected.column))
// {
//     Field actual_value = assert_cast<const ColumnConst &>(*actual.column).getField();
//     Field expected_value = assert_cast<const ColumnConst &>(*expected.column).getField();

//     if (actual_value != expected_value)
//         return on_error("Block structure mismatch in " + context_description + " stream: different values of constants, actual: "
//             + applyVisitor(FieldVisitorToString(), actual_value) + ", expected: " + applyVisitor(FieldVisitorToString(), expected_value),
//             ErrorCodes::BLOCKS_HAVE_DIFFERENT_STRUCTURE);
// }
//     }

//     return ReturnType(true);
// }

// bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs)
// {
//     return checkBlockStructure<bool>(lhs, rhs, {});
// }

// void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, const std::string & context_description)
// {
//     checkBlockStructure<void>(lhs, rhs, context_description);
// }

// void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff)
// {
//     /// The traditional task: the largest common subsequence (LCS).
//     /// Assume that order is important. If this becomes wrong once, let's simplify it: for example, make 2 sets.

//     std::vector<std::vector<int>> lcs(lhs.columns() + 1);
//     for (auto & v : lcs)
//         v.resize(rhs.columns() + 1);

//     for (size_t i = 1; i <= lhs.columns(); ++i)
//     {
//         for (size_t j = 1; j <= rhs.columns(); ++j)
//         {
//             if (lhs.safeGetByPosition(i - 1) == rhs.safeGetByPosition(j - 1))
//                 lcs[i][j] = lcs[i - 1][j - 1] + 1;
//             else
//                 lcs[i][j] = std::max(lcs[i - 1][j], lcs[i][j - 1]);
//         }
//     }

//     /// Now go back and collect the answer.
//     ColumnsWithTypeAndName left_columns;
//     ColumnsWithTypeAndName right_columns;
//     size_t l = lhs.columns();
//     size_t r = rhs.columns();
//     while (l > 0 && r > 0)
//     {
//         if (lhs.safeGetByPosition(l - 1) == rhs.safeGetByPosition(r - 1))
//         {
//             /// This element is in both sequences, so it does not get into `diff`.
//             --l;
//             --r;
//         }
//         else
//         {
//             /// Small heuristics: most often used when getting a difference for (expected_block, actual_block).
//             /// Therefore, the preference will be given to the field, which is in the left block (expected_block), therefore
//             /// in `diff` the column from `actual_block` will get.
//             if (lcs[l][r - 1] >= lcs[l - 1][r])
//                 right_columns.push_back(rhs.safeGetByPosition(--r));
//             else
//                 left_columns.push_back(lhs.safeGetByPosition(--l));
//         }
//     }

//     while (l > 0)
//         left_columns.push_back(lhs.safeGetByPosition(--l));
//     while (r > 0)
//         right_columns.push_back(rhs.safeGetByPosition(--r));

//     WriteBufferFromString lhs_diff_writer(out_lhs_diff);
//     WriteBufferFromString rhs_diff_writer(out_rhs_diff);

//     for (auto it = left_columns.rbegin(); it != left_columns.rend(); ++it)
//     {
//         lhs_diff_writer << it->dumpStructure();
//         lhs_diff_writer << ", position: " << lhs.getPositionByName(it->name) << '\n';
//     }
//     for (auto it = right_columns.rbegin(); it != right_columns.rend(); ++it)
//     {
//         rhs_diff_writer << it->dumpStructure();
//         rhs_diff_writer << ", position: " << rhs.getPositionByName(it->name) << '\n';
//     }
// }

void Block::clear() {
    info = BlockInfo();
    data.clear();
    index_by_name.clear();
}

void Block::swap(Block& other) noexcept {
    std::swap(info, other.info);
    data.swap(other.data);
    index_by_name.swap(other.index_by_name);
}

void Block::updateHash(SipHash& hash) const {
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no)
        for (const auto& col : data) col.column->updateHashWithValue(row_no, hash);
}

void Block::filter_block(Block* block, int filter_column_id, int column_to_keep) {
    ColumnPtr filter_column = block->getByPosition(filter_column_id).column;
    const IColumn::Filter& filter =
            assert_cast<const doris::vectorized::ColumnVector<UInt8>&>(*filter_column).getData();

    auto count = countBytesInFilter(filter);
    if (count == 0) {
        block->getByPosition(0).column = block->getByPosition(0).column->cloneEmpty();
    } else {
        if (count != block->rows()) {
            for (int i = 0; i < column_to_keep; ++i) {
                block->getByPosition(i).column = block->getByPosition(i).column->filter(filter, 0);
            }
        }
        for (size_t i = column_to_keep; i < block->columns(); ++i) {
            block->erase(i);
        }
    }
}
void Block::serialize(PBlock* pblock) const {
    for (auto c = cbegin(); c != cend(); ++c) {
        PColumn* pc = pblock->add_columns();
        pc->set_name(c->name);
        if (c->type->isNullable()) {
            pc->set_type(get_pdata_type(
                    std::dynamic_pointer_cast<const DataTypeNullable>(c->type)->getNestedType()));
        } else {
            pc->set_type(get_pdata_type(c->type));
        }
        c->type->serialize(*(c->column), pc);
    }
}

int MutableBlock::rows() {
    for (const auto& column : _columns)
        if (column) return column->size();

    return 0;
}

void MutableBlock::add_row(const Block* block, int row) {
    auto& src_columns_with_schema = block->getColumnsWithTypeAndName();
    for (int i = 0; i < _columns.size(); ++i) {
        _columns[i]->insertFrom(*src_columns_with_schema[i].column.get(), row);
    }
}

Block MutableBlock::to_block() {
    ColumnsWithTypeAndName columns_with_schema;
    for (int i = 0; i < _columns.size(); ++i) {
        columns_with_schema.emplace_back(std::move(_columns[i]), _data_types[i], "");
    }
    return columns_with_schema;
}

} // namespace doris::vectorized
