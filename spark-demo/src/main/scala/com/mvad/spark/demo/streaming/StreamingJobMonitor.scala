package com.mvad.spark.demo.streaming

import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory

/**
  * Monitoring Spark Streaming Job
  * put job info/progress to zookeeper
  */
class StreamingJobMonitor(sparkConf: SparkConf) extends StreamingListener {
  val log = LoggerFactory.getLogger(this.getClass)

  // initialize zkClient for job monitoring
  val zkConnect = "zk1ss.prod.mediav.com:2191,zk2ss.prod.mediav.com:2191,zk3ss.prod.mediav.com:2191,zk12ss.prod.mediav.com:2191,zk13ss.prod.mediav.com:2191"
  val zkSessionTimeoutMs = 6000
  val zkConnectionTimeoutMs = 6000
  val zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, ZKStringSerializer)
  val zkAppPath = s"/streaming/${sparkConf.get("spark.app.name")}"
  log.info(s"Initializing Job monitor ZK Path, zkConnect: ${zkConnect} ; " +
    s"zkSessionTimeoutMs: ${zkSessionTimeoutMs} ; zkConnectionTimeoutMs: ${zkConnectionTimeoutMs} ; zkAppPath: ${zkAppPath}")

  val receivers = s"${zkAppPath}/receiver"
  val jobProgress = s"${zkAppPath}/progress"

  log.info(s"Initializing StreamingJobMonitor , zk Path : ${zkAppPath}...")
  ZkUtils.makeSurePersistentPathExists(zkClient, receivers)
  ZkUtils.makeSurePersistentPathExists(zkClient, jobProgress)

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    val receiver = s"${receivers}/${receiverStarted.receiverInfo.name}"
    ZkUtils.makeSurePersistentPathExists(zkClient, receiver)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    val receiver = s"${receivers}/${receiverError.receiverInfo.name}"
    ZkUtils.updatePersistentPath(zkClient, receiver, "ERROR")
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    val receiver = s"${receivers}/${receiverStopped.receiverInfo.name}"
    ZkUtils.deletePath(zkClient, receiver)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val progressInfo = s"{lastBatchEndTime:${batchCompleted.batchInfo.processingEndTime.getOrElse(0)}," +
      s"schedulingDelay:${batchCompleted.batchInfo.schedulingDelay.getOrElse(0)}," +
      s"processingDelay:${batchCompleted.batchInfo.processingDelay.getOrElse(0)}," +
      s"totalDelay:${batchCompleted.batchInfo.totalDelay.getOrElse(0)}}"
    ZkUtils.updatePersistentPath(zkClient, jobProgress, progressInfo)
  }
}
