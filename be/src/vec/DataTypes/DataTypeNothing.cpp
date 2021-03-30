#include <vec/Common/typeid_cast.h>
#include <vec/DataTypes/DataTypeNothing.h>
// #include <vec/DataTypes/DataTypeFactory.h>
#include <vec/Columns/ColumnNothing.h>
// #include <vec/IO/ReadBuffer.h>
// #include <vec/IO/WriteBuffer.h>


namespace DB
{

MutableColumnPtr DataTypeNothing::createColumn() const
{
    return ColumnNothing::create(0);
}

// void DataTypeNothing::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
// {
//     size_t size = column.size();

//     if (limit == 0 || offset + limit > size)
//         limit = size - offset;

//     for (size_t i = 0; i < limit; ++i)
//         ostr.write('0');
// }

// void DataTypeNothing::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double /*avg_value_size_hint*/) const
// {
//     typeid_cast<ColumnNothing &>(column).addSize(istr.tryIgnore(limit));
// }

bool DataTypeNothing::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}


// void registerDataTypeNothing(DataTypeFactory & factory)
// {
//     factory.registerSimpleDataType("Nothing", [] { return DataTypePtr(std::make_shared<DataTypeNothing>()); });
// }

}
