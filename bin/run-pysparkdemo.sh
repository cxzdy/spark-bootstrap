#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

PYSPARK_SCRIPT="$FWDIR"/python/pysparkdemo.py

LOGDIR="$FWDIR"/logs

spark-submit --name 'Spark:[com.mvad.spark.demo][PySpark]' --queue bi $PYSPARK_SCRIPT > $LOGDIR/pyspark-demo.log


