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
--conf spark.cores.max=700 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=180s \
--conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.streaming.kafka.maxRatePerPartition=2000 \
--conf spark.streaming.blockInterval=500ms \
--conf spark.streaming.backpressure.enabled=true \
--driver-memory 5G \
--executor-cores 1 --executor-memory 3g \
--class com.mvad.spark.demo.streaming.DSPRealTimeSessionization $SPARK_JAR DSPRealTimeSessionization d.u.6,d.u.6.m,d.s.6,d.s.6.m,d.c.6,d.c.6.m 5 dspsession



