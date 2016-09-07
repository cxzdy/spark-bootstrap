package com.mvad.spark.demo.streaming

import kafka.utils._
import org.apache.spark.streaming.scheduler._
import org.slf4j.LoggerFactory


/**
  * Monitoring Spark Streaming Job
  * put job info/progress to zookeeper
  */
class StreamingJobMonitor(zkUtils: ZkUtils, zkAppPath: String) extends StreamingListener {
  val log = LoggerFactory.getLogger(this.getClass)

  val receivers = s"${zkAppPath}/receiver"
  val jobProgress = s"${zkAppPath}/progress"

  log.info(s"Initializing StreamingJobMonitor , zk Path : ${zkAppPath}...")
  zkUtils.makeSurePersistentPathExists(receivers)
  zkUtils.makeSurePersistentPathExists(jobProgress)

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    val receiver = s"${receivers}/${receiverStarted.receiverInfo.name}"
    zkUtils.makeSurePersistentPathExists(receiver)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    val receiver = s"${receivers}/${receiverError.receiverInfo.name}"
    zkUtils.updatePersistentPath(receiver, "ERROR")
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    val receiver = s"${receivers}/${receiverStopped.receiverInfo.name}"
    zkUtils.deletePath(receiver)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val progressInfo = s"{totalDelay:${batchCompleted.batchInfo.totalDelay.getOrElse(0)}," +
      s"schedulingDelay:${batchCompleted.batchInfo.schedulingDelay.getOrElse(0)}," +
      s"processingDelay:${batchCompleted.batchInfo.processingDelay.getOrElse(0)}"
    zkUtils.updatePersistentPath(jobProgress, progressInfo)
  }
}
