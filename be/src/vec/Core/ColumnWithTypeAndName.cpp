#include <vec/Core/ColumnsWithTypeAndName.h>
#include <ostream>
#include <sstream>
//#include <IO/WriteBufferFromString.h>
//#include <IO/WriteHelpers.h>
//#include <IO/Operators.h>


namespace DB
{

ColumnWithTypeAndName ColumnWithTypeAndName::cloneEmpty() const
{
    ColumnWithTypeAndName res;

    res.name = name;
    res.type = type;
    if (column)
        res.column = column->cloneEmpty();

    return res;
}


bool ColumnWithTypeAndName::operator==(const ColumnWithTypeAndName & other) const
{
    return name == other.name
        && ((!type && !other.type) || (type && other.type && type->equals(*other.type)))
        && ((!column && !other.column) || (column && other.column && column->getName() == other.column->getName()));
}


void ColumnWithTypeAndName::dumpStructure(std::ostream & out) const
{
    out << name;

    if (type)
        //out << ' ' << type->getName();
        out << " ";
    else
        out << " nullptr";

    if (column)
        out << ' ' << column->dumpStructure();
    else
        out << " nullptr";
}

String ColumnWithTypeAndName::dumpStructure() const
{
    std::stringstream out;
    dumpStructure(out);
    return out.str();
}

}
