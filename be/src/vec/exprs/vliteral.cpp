#include "vec/exprs/vliteral.h"

#include "fmt/format.h"
#include "util/string_parser.hpp"
#include "vec/columns/column.h"
#include "vec/core/field.h"

namespace doris::vectorized {
VLiteral::VLiteral(const TExprNode& node) : VExpr(node) {
    Field field;
    switch (_type.type) {
    case TYPE_BOOLEAN: {
        DCHECK_EQ(node.node_type, TExprNodeType::BOOL_LITERAL);
        DCHECK(node.__isset.bool_literal);
        field = Int8(node.bool_literal.value);
        break;
    }
    case TYPE_TINYINT: {
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        field = Int8(node.int_literal.value);
        break;
    }
    case TYPE_SMALLINT: {
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        field = Int16(node.int_literal.value);
        break;
    }
    case TYPE_INT: {
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        field = Int32(node.int_literal.value);
        break;
    }
    case TYPE_BIGINT: {
        DCHECK_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        DCHECK(node.__isset.int_literal);
        field = Int64(node.int_literal.value);
        break;
    }
    case TYPE_LARGEINT: {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        DCHECK_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);
        __int128_t value = StringParser::string_to_int<__int128>(
                node.large_int_literal.value.c_str(), node.large_int_literal.value.size(),
                &parse_result);
        if (parse_result != StringParser::PARSE_SUCCESS) {
            value = MAX_INT128;
        }
        field = Int128(value);
        break;
    }
    case TYPE_FLOAT: {
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        field = Float32(node.float_literal.value);
        break;
    }
    // case TYPE_TIME:
    case TYPE_DOUBLE: {
        DCHECK_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        DCHECK(node.__isset.float_literal);
        field = Float64(node.float_literal.value);
        break;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        DateTimeValue value;
        value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size());
        field = Int128(*reinterpret_cast<__int128_t*>(&value));
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        DCHECK_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        DCHECK(node.__isset.string_literal);
        field = node.string_literal.value;
        break;
    }
    case TYPE_DECIMAL: {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        DecimalValue value(node.decimal_literal.value);
        // TODO: fix here
        DCHECK(false) << "not support decimal V1";
        break;
    }
    case TYPE_DECIMALV2: {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        DecimalV2Value value(node.decimal_literal.value);
        field = DecimalField<Decimal128>(value.value(), value.scale());
        break;
    }
    default: {
        DCHECK(false) << "Invalid type: " << _type.type;
        break;
    }
    }
    this->_column_ptr = _data_type->createColumnConst(1, field);
    _expr_name = _data_type->getName();
}

VLiteral::~VLiteral() {}

Status VLiteral::execute(vectorized::Block* block, int* result_column_id) {
    int rows = block->rows();
    size_t res = block->columns();
    block->insert({_column_ptr->cloneResized(rows), _data_type, _expr_name});
    *result_column_id = res;
    return Status::OK();
}
} // namespace doris::vectorized
