//#include <IO/WriteBufferFromString.h>
//#include <IO/Operators.h>
#include <sstream>
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_const.h"
#include "vec/core/field.h"


namespace DB
{

std::string IColumn::dumpStructure() const
{
    std::stringstream res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    };

    const_cast<IColumn*>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

}
