package com.mvad.spark.demo.streaming

import java.util
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
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Usage:
  * spark-submit --class \
  * com.mvad.spark.demo.hbase.DSPRealTimeSessionization d.u.6,d.u.6.m,d.s.6,d.s.6.m,d.c.6,d.c.6.m 5 dspsession
  */
object DSPRealTimeSessionization {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: RealTimeSessionization <topics> <batchInteval> <htablename>")
      System.exit(1)
    }

    log.info(s"Starting SparkStreaming Job : ${this.getClass.getName}")

    val Array(topics, batchInteval, htablename) = args

    val appName = this.getClass.getSimpleName
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
    ssc.checkpoint(s"checkpoint-${appName}")


    // initialize zkClient for job monitoring
    val zkConnect = "zk1ss.prod.mediav.com:2191,zk2ss.prod.mediav.com:2191,zk3ss.prod.mediav.com:2191,zk12ss.prod.mediav.com:2191,zk13ss.prod.mediav.com:2191"
    val zkSessionTimeoutMs = 6000
    val zkConnectionTimeoutMs = 6000
    val zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs)
    val zkUtils = ZkUtils(zkClient, false)
    val zkAppPath = s"/streaming/${appName}"
    log.info(s"Initializing Job monitor ZK Path, zkConnect: ${zkConnect} ; " +
      s"zkSessionTimeoutMs: ${zkSessionTimeoutMs} ; zkConnectionTimeoutMs: ${zkConnectionTimeoutMs} ; zkAppPath: ${zkAppPath}")

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

    ssc.addStreamingListener(new StreamingJobMonitor(zkUtils, zkAppPath))
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        zkUtils.deletePath(zkAppPath)
      }
    })
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

            //            if (eventType == 200 || eventType == 115) {
            //              // .u or .s flatten impressionInfos
            //              val impressionInfos = ue.getAdvertisementInfo.getImpressionInfos
            //              impressionInfos.map(impressionInfo => {
            //                val showRequestId = impressionInfo.getShowRequestId
            //                val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"
            //
            //                val put = new Put(Bytes.toBytes(rowkey))
            //                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
            //                (new ImmutableBytesWritable, put)
            //              })
            //            } else {
            //              // .c
            //              val impressionInfo = ue.getAdvertisementInfo.getImpressionInfo
            //              val showRequestId = impressionInfo.getShowRequestId
            //              val rowkey = s"${showRequestId.toString.hashCode}#${showRequestId.toString}"
            //
            //              val put = new Put(Bytes.toBytes(rowkey))
            //              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
            //              Seq((new ImmutableBytesWritable, put))
            //            }
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

    }

    )


  }

}

