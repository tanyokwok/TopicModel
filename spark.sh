#encoding=utf-8
#########################################################################
# File Name: spark.sh
# Author: GuoTianyou
# mail: fortianyou@gmail.com
# Created Time: Thu Mar  9 09:54:47 2017
#! /bin/bash

#########################################################################

$SPARK_HOME/bin/spark-submit \
	  --master local[2] \
	    --class "bda.spark.topic.task.DistributedParallelLDATask" \
		  out/artifacts/lda_jar/lda.jar
