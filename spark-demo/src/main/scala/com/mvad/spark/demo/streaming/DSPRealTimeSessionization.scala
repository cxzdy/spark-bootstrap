package com.mvad.spark.demo.streaming

import java.util.Base64

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Usage:
  * spark-submit --class \
  * com.mvad.spark.demo.hbase.DSPRealTimeSessionization DSPRealTimeSessionization d.u.6,d.u.6.m,d.s.6,d.s.6.m,d.c.6,d.c.6.m 5 dspsession
  */
object DSPRealTimeSessionization {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    if (args.length != 4) {
      System.err.println(s"Usage: ${this.getClass.getCanonicalName} <appName> <topics> <batchInteval> <htablename>")
      System.exit(1)
    }

    log.info(s"Starting SparkStreaming Job : ${this.getClass.getName}")

    val Array(appName, topics, batchInteval, htablename) = args
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)

    try {

      val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
      ssc.checkpoint(s"checkpoint-${appName}")

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
      conf.setLong("hbase.client.write.buffer", 3 * 1024 * 1024)
      val hbaseContext = new HBaseContext(sc, conf)

      /* using direct mode */
      val topicsSet = topics.split(",").toSet
      val brokers = "kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092"
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> appName,
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      log.info(s"Initializing kafka Receiver, topics: ${topics} ; brokers: ${brokers} ; kafkaParams: ${kafkaParams.mkString(",")} ")

      val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

      kafkaStream.foreachRDD { rdd =>
        val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val puts = rdd.map(record => record.value).flatMap(line => {

          val raw = Base64.getDecoder.decode(line)
          val ue = com.mediav.data.log.LogUtils.ThriftBase64decoder(line, classOf[com.mediav.data.log.unitedlog.UnitedEvent])
          val eventType = ue.getEventType.getType
          val family = if (eventType == 200) "u"
          else if (eventType == 115) "s"
          else if (eventType == 99) "c"
          else throw new SparkException(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

          // got all impressions
          val impressionInfos = if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfos || ue.getAdvertisementInfo.isSetImpressionInfo)) {
            if (eventType == 200 || eventType == 115) {
              ue.getAdvertisementInfo.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
            } else {
              Seq(ue.getAdvertisementInfo.getImpressionInfo).toList
            }
          } else {
            Seq()
          }

          // transform to put format
          val putRecords = impressionInfos.map(impressionInfo => {
            val showRequestId = impressionInfo.getShowRequestId
            val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

            val put = new Put(Bytes.toBytes(rowkey))
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
            // put format : (Array[Byte], Array[(Array[Byte], Array[Byte], Long, Array[Byte])])
            (Bytes.toBytes(rowkey), Array((Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)))
          })
          putRecords
        })

        hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Long, Array[Byte])])](puts, TableName.valueOf(htablename), (putRecord) => {
          val put = new Put(putRecord._1)
          putRecord._2.foreach((putValue) =>
            put.addColumn(putValue._1, putValue._2, putValue._3, putValue._4))
          put
        })

        // commit offsets back to kafka
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
      }

      ssc.addStreamingListener(new StreamingJobMonitor(sparkConf))
      ssc.start()
      ssc.awaitTermination()
    } finally {
      sc.stop()
    }
  }

}

