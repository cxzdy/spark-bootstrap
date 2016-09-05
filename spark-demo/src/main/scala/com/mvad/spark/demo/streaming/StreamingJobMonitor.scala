package com.mvad.spark.demo.streaming

import org.apache.spark.streaming.scheduler._

/**
  * Monitoring Spark Streaming Job
  * put job info/progress to zookeeper
  */
class StreamingJobMonitor extends StreamingListener {
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    super.onReceiverStarted(receiverStarted)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    super.onReceiverError(receiverError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    super.onReceiverStopped(receiverStopped)
  }

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    super.onBatchSubmitted(batchSubmitted)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    super.onBatchStarted(batchStarted)
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    super.onBatchCompleted(batchCompleted)
    batchCompleted.batchInfo.totalDelay
  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = {
    super.onOutputOperationStarted(outputOperationStarted)
  }

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = {
    super.onOutputOperationCompleted(outputOperationCompleted)
  }
}
