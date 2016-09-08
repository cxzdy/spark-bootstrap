package com.mvad.spark.demo.streaming

import java.util.Base64

import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.hbase.async.{HBaseClient, PutRequest}
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
    val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
    ssc.checkpoint(s"checkpoint-${appName}")


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

    saveToHBase(kafkaStream, htablename)

    ssc.addStreamingListener(new StreamingJobMonitor(ssc))
    ssc.start()
    ssc.awaitTermination()
  }

  def saveToHBase(stream: DStream[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]], htablename: String) = {
    stream.foreachRDD(rdd => {

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)

      /* convert to Put and save to HBase */
      try {
        val puts = rdd.map(record => record.value).flatMap(line => {
          val raw = Base64.getDecoder.decode(line)
          val ue = com.mediav.data.log.LogUtils.ThriftBase64decoder(line, classOf[com.mediav.data.log.unitedlog.UnitedEvent])
          if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfo || ue.getAdvertisementInfo.isSetImpressionInfos)) {
            val eventType = ue.getEventType.getType
            val family = if (eventType == 200) "u"
            else if (eventType == 115) "s"
            else if (eventType == 99) "c"
            else throw new SparkException(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

            val ad = ue.getAdvertisementInfo
            val impressionInfos = if (eventType == 200 || eventType == 115) {
              ad.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
            } else {
              Seq(ad.getImpressionInfo).toList
            }

            impressionInfos.map(impressionInfo => {
              val showRequestId = impressionInfo.getShowRequestId
              val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

              val put = new Put(Bytes.toBytes(rowkey))
              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
              (new ImmutableBytesWritable, put)
            })
          } else {
            Seq()
          }
        })

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
        conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
        conf.setLong("hbase.client.write.buffer", 3 * 1024 * 1024)
        conf.set(TableOutputFormat.OUTPUT_TABLE, htablename)
        puts.saveAsNewAPIHadoopDataset(conf)
      } catch {
        case ex: NullPointerException => log.warn("NPE: ", ex)
      }

    })

  }

  def asyncSaveToHBase(stream: DStream[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]], htablename: String) = {
    stream.foreachRDD(rdd => {

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)

      rdd.map(record => record.value).foreachPartition(recordsPerPartiion => {
        val client = new HBaseClient("nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")

        recordsPerPartiion.foreach(line => {
          val raw = Base64.getDecoder.decode(line)
          val ue = com.mediav.data.log.LogUtils.thriftBinarydecoder(raw, classOf[com.mediav.data.log.unitedlog.UnitedEvent])
          if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfo || ue.getAdvertisementInfo.isSetImpressionInfos)) {
            val eventType = ue.getEventType.getType
            val family = if (eventType == 200) "u"
            else if (eventType == 115) "s"
            else if (eventType == 99) "c"
            else throw new SparkException(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

            val ad = ue.getAdvertisementInfo
            val impressionInfos = if (eventType == 200 || eventType == 115) {
              ad.getImpressionInfos.asInstanceOf[java.util.ArrayList[com.mediav.data.log.unitedlog.ImpressionInfo]].toList
            } else {
              Seq(ad.getImpressionInfo).toList
            }

            impressionInfos.foreach(impressionInfo => {
              val showRequestId = impressionInfo.getShowRequestId
              val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"

              val putReq = new PutRequest(Bytes.toBytes(htablename), Bytes.toBytes(rowkey), Bytes.toBytes(family), Bytes.toBytes(ue.getLogId),
                raw, ue.getEventTime)
              client.put(putReq)
            })
          }
        })

        client.shutdown().join()
      })
    })
  }

}

