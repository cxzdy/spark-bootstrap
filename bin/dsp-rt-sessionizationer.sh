#!/bin/bash

FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

set -e

SPARK_JAR=`ls "$FWDIR"/lib/spark-demo*.jar`

LOGDIR="$FWDIR"/logs
export SPARK_HOME=/opt/spark-2.0.0-cdh5.8.0
export SPARK_CONF_DIR=/etc/spark/conf.ss-spark-2.0

$SPARK_HOME/bin/spark-submit \
--master yarn \
--deploy-mode client \
--conf spark.speculation=true \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.shuffle.service.enabled=false \
--conf spark.cores.max=1000 \
--conf spark.streaming.concurrentJobs=1 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=180s \
--conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
--conf spark.streaming.stopGracefullyOnShutdown=true \
--conf spark.streaming.kafka.maxRatePerPartition=1000 \
--conf spark.streaming.blockInterval=500ms \
--conf spark.streaming.backpressure.enabled=true \
--conf spark.streaming.kafka.consumer.poll.ms=5000 \
--conf spark.streaming.kafka.consumer.cache.initialCapacity=1000 \
--conf spark.streaming.kafka.consumer.cache.maxCapacity=64000 \
--driver-memory 15G \
--num-executors 1000 \
--executor-cores 1 --executor-memory 20g \
--class com.mvad.spark.demo.streaming.DSPRealTimeSessionization $SPARK_JAR DSPRealTimeSessionizationOnYarn20160909 d.u.6,d.u.6.m,d.s.6,d.s.6.m,d.c.6,d.c.6.m 5 dspsession



