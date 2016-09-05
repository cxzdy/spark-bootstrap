package com.mvad.spark.demo.hbase

import java.util
import java.util.Base64

import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog.UnitedEvent
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
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

    val Array(topics, batchInteval, htablename) = args
    val sparkConf = new SparkConf().setAppName("DSPRealTimeSessionization")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
    ssc.checkpoint("checkpoint-DSPRealTimeSessionization")

    /* using receiver mode */
    //    val topicMap = topics.split(",").map((_, numThreadsPerReceiver.toInt)).toMap
    //    val kafkaStreams = (1 to numReceivers.toInt).map {
    //      i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //    }
    //    val unifiedStream = ssc.union(kafkaStreams)
    //    val lines = unifiedStream.map(_._2)

    // Kafka configurations
    val topicsSet = topics.split(",").toSet
    val brokers = "kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = kafkaStream.map(_._2)
    val impressions = lines.flatMap(toImpressions)

    impressions.foreachRDD((rdd: RDD[(Long, Array[Byte])], time: Time) => {
      try {
        val events = rdd.map(toPut)

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
        conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
        conf.set(TableOutputFormat.OUTPUT_TABLE, htablename)
        events.saveAsNewAPIHadoopDataset(conf)
      } catch {
        case ex: NullPointerException => log.warn("NPE: ", ex)
      }
    })

    /*
    impressions.foreachRDD((rdd: RDD[(Long, String)], time: Time) => {

      rdd.foreachPartition(partitionOfRecords => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")

        val conn = ConnectionFactory.createConnection(conf)
        val table = new HTable(conf, TableName.valueOf(htablename))
        //        table.setAutoFlush(false, false)
        table.setWriteBufferSize(3 * 1024 * 1024)

        partitionOfRecords.foreach(pair => {
          val put = toPut(pair)
          table.put(put._2)
        })
        table.flushCommits()
        table.close()
      })
    })
    */

    ssc.start()
    ssc.awaitTermination()
  }

  // flatten log to get all impressions
  def toImpressions(line: String) = {
    val impList = new util.ArrayList[(Long, Array[Byte])]()

    val raw = Base64.getDecoder.decode(line)
    val event = LogUtils.ThriftBase64decoder(line, classOf[com.mediav.data.log.unitedlog.UnitedEvent])

    val eventType = event.getEventType.getType
    if (eventType == 200 || eventType == 115) {
      // d.u or d.s
      val impressionInfos = event.getAdvertisementInfo.getImpressionInfos
      if (impressionInfos != null) {
        for (impressionInfo <- impressionInfos) {
          impList.add((impressionInfo.getShowRequestId, raw))
        }
      }
    } else if (eventType == 99) {
      // d.c
      val impressionInfo = event.getAdvertisementInfo.getImpressionInfo
      impList.add((impressionInfo.getShowRequestId, raw))
    }

    impList
  }

  // convert to hbase put
  def toPut(pair: (Long, Array[Byte])) = {

    // showRequestId's hashcode used for rowkey for random distributed
    val rowkey = pair._1.toString.hashCode

    val raw = pair._2
    val event = LogUtils.thriftBinarydecoder(raw, classOf[UnitedEvent])
    val logId = event.getLogId
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val put = new Put(Bytes.toBytes(rowkey))

    if (eventType == 200) {
      // e.u
      put.addColumn(Bytes.toBytes("u"), Bytes.toBytes(logId), ts, raw)
    } else if (eventType == 115) {
      // e.s
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes(logId), ts, raw)
    } else if (eventType == 99) {
      // e.c
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes(logId), ts, raw)
    }

    (new ImmutableBytesWritable, put)
  }

}

