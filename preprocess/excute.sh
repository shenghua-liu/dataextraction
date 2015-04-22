#!/bin/bash
# DIR=/Users/zhyueqi/WorkSpace/spark/data/
#清理节点
if [ $1 -eq "1" ]; then
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 1 "/Users/zhyueqi/WorkSpace/spark/data/post_test.csv" "/Users/zhyueqi/WorkSpace/spark/data/post_test_result"
fi
# #清理边
# if [ $1 -eq "2" ]; then
# ./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar \
# 2 \
# $DIR"reRelation_test.csv" \
# $DIR"reRelation_test_result" \
# fi
# #恢复ID
# if [ $1 -eq "3" ]; then
# ./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar \
# 3 \
# $DIR"post_test_result.csv" \
# $DIR"reRelation_test_result.csv" \
# $DIR"recover_reRelation" \
# fi
# #求子图
# if [ $1 -eq "4" ]; then
# ./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar \
# 4 \
# $DIR"recover_reRelation.csv" \
# $DIR"components" \
# fi