ANS=' s/<Core/<vec\/Core/g  s/<Common/<vec\/Common/g s/<common/<vec\/common/g s/<Columns/<vec\/Columns/g s/<DataTypes/<vec\/DataTypes/g s/<AggregateFunctions/<vec\/AggregateFunctions/g'
for i in $ANS;do
find . -name "*.h" |xargs sed -i $i
done
#find -name "*.cpp|*.h" |xargs sed -i "s/<Core/<vec\/Core/g"

#/home/users/fenghaoan/opt/workspace/doris/CK/baidu/personal-code/doris-benchmark/src/AggregateFunctions
