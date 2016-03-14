#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

SPARK_JAR=`ls "$FWDIR"/lib/spark-demo*.jar`

LOGDIR="$FWDIR"/logs

spark-submit --class com.mvad.spark.demo.sql.SessionLogParquetDemo --name 'SparkSQL:[com.mvad.spark.demo][SessionLogParquetDemo]' --queue bi $SPARK_JAR > $LOGDIR/spark-demo-sessionlog.log


