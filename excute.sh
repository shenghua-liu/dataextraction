#!/bin/bash
LOCAL=true
###IF LOCAL FILE SYSTEM
DIR=/Users/zhyueqi/WorkSpace/spark/data
###IF HDFS
#DIR=hdfs://
####################所有程序最后会合并文件
#清理节点
if [ $1 == "1" ]; then
	arg1=$DIR/post_test.csv
	arg2=$DIR/post_test_result
	if [ LOCAL ]; then	
		rm -r $arg2
		rm $arg2.csv
	fi
#参数1：定点文件，参数2：输出路径（文件夹）
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 1 $arg1 $arg2
fi
#清理边
if [ $1 == "2" ]; then
	arg1=$DIR/reRelation_test.csv
	arg2=$DIR/reRelation_test_result
	if [ LOCAL ]; then
		rm -r $arg2
		rm $arg2.csv
	fi
#参数1：边文件，参数2：输出路径（文件夹）
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 2 $arg1  $arg2
fi
#恢复ID
if [ $1 == "3" ]; then
	arg1=$DIR/post_test_result.csv
	arg2=$DIR/reRelation_test_result.csv
	arg3=$DIR/recover_reRelation
	if [ LOCAL ]; then
		rm -r $arg3
		rm $arg3.csv
	fi
#参数1：去重后的定点文件；#参数2：去重后的边文件；#参数3；保存路径（文件夹）
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 3 $arg1 $arg2 $arg3
fi
#求子图	模式1
if [ $1 == "4" ]; then
	arg1=$DIR/recover_reRelation.csv
	arg2=$DIR/components
	if [ LOCAL ]; then
		rm -r $arg2
		rm $arg2.txt
	fi
#参数1：去重后的边文件；#参数2：保存路径（文件夹）
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 4 $arg1 $arg2
fi
#求子图 模式2
if [ $1 == "5" ]; then
	arg1=$DIR/recover_reRelation.csv
	arg2=$DIR/components
	arg3=$DIR/post_test_result.csv
	if [ LOCAL ]; then
		rm -r $arg2
		rm $arg2.txt
	fi
#参数1：去重后的边文件；#参数2：保存路径（文件夹）；#参数3：去重后的顶点文件
./spark-submit --class "SubGraph" --master local[2] ./deploy/subgraph-project_2.10-1.0.jar 4 $arg1 $arg2 $arg3
fi
