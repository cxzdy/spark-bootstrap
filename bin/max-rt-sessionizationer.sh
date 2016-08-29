#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

SPARK_JAR=`ls "$FWDIR"/lib/spark-demo*.jar`

LOGDIR="$FWDIR"/logs
export SPARK_HOME=/opt/spark-2.0.0-cdh5.8.0
export SPARK_CONF_DIR=/etc/spark/conf.ss-spark-2.0

$SPARK_HOME/bin/spark-submit \
--conf spark.speculation=true \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.cores.max=300 \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.streaming.kafka.maxRatePerPartition=2000 \
--conf spark.streaming.blockInterval=500ms \
--conf spark.streaming.backpressure.enabled=true \
--driver-memory 5G \
--executor-cores 1 --executor-memory 3g \
--class com.mvad.spark.demo.hbase.MaxRealTimeSessionization $SPARK_JAR e.u.6,e.s.6.e.c.6 5 maxsession



