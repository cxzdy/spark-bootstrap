package com.mvad.spark.demo.streaming

import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory

/**
  * Monitoring Spark Streaming Job
  * put job info/progress to zookeeper
  */
class StreamingJobMonitor(ssc: StreamingContext, zkClient: ZkClient) extends StreamingListener {
  val log = LoggerFactory.getLogger(this.getClass)
  log.info("Initializing StreamingJobMonitor ...")

  val zkRootPath = s"/streaming/${ssc.sparkContext.getConf.get("spark.app.name")}/${ssc.sparkContext.getConf.getAppId}"
  val receivers = s"${zkRootPath}/receiver"
  val jobProgress = s"${zkRootPath}/progress"
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
    val progressInfo = s"{totalDelay:${batchCompleted.batchInfo.totalDelay.getOrElse(0)}," +
      s"schedulingDelay:${batchCompleted.batchInfo.schedulingDelay.getOrElse(0)}," +
      s"processingDelay:${batchCompleted.batchInfo.processingDelay.getOrElse(0)}"
    ZkUtils.updatePersistentPath(zkClient, jobProgress, progressInfo)
  }
}
