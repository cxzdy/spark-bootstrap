package com.mvad.spark.demo.streaming

import java.util
import java.util.Base64

import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog.UnitedEvent
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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

    log.info(s"Starting SparkStreaming Job : ${this.getClass.getName}")

    val Array(topics, batchInteval, htablename) = args
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
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
    log.info(s"Initializing kafka Receiver, topics: ${topics} ; brokers: ${brokers} ; kafkaParams: ${kafkaParams.mkString(",")} ")

    val zkConnect = "zk1ss.prod.mediav.com:2191,zk2ss.prod.mediav.com:2191,zk3ss.prod.mediav.com:2191,zk12ss.prod.mediav.com:2191,zk13ss.prod.mediav.com:2191"
    val zkSessionTimeoutMs = 6000
    val zkConnectionTimeoutMs = 6000

    val zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, ZKStringSerializer)
    val zkRootPath = s"/streaming/${sc.getConf.get("spark.app.name")}/${sc.getConf.getAppId}"
    log.info(s"Initializing Job monitor ZK Path, zkConnect: ${zkConnect} ; " +
      s"zkSessionTimeoutMs: ${zkSessionTimeoutMs} ; zkConnectionTimeoutMs: ${zkConnectionTimeoutMs} ; zkRootPath: ${zkRootPath}")
    ZkUtils.makeSurePersistentPathExists(zkClient, zkRootPath)

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    var offsetRanges = Array[OffsetRange]()
    val lines = kafkaStream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }.map(_._2)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      try {
        // record offset to zk
        for (o <- offsetRanges) {
          log.info(s"${o.topic}, ${o.partition}, ${o.fromOffset}, ${o.untilOffset}")
          //          val zkClient = new ZkClient(zkConnect, zkSessionTimeoutMs, zkConnectionTimeoutMs, ZKStringSerializer)
          //          val path = s"${zkRootPath}/offset/${o.topic}/${o.partition}"
          //          ZkUtils.makeSurePersistentPathExists(zkClient, path)
          //          ZkUtils.updatePersistentPath(zkClient, path, o.fromOffset.toString)
          //          zkClient.close()
        }

        val puts = rdd.flatMap(line => {
          val raw = Base64.getDecoder.decode(line)
          val ue = LogUtils.ThriftBase64decoder(line, classOf[com.mediav.data.log.unitedlog.UnitedEvent])
          if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfo || ue.getAdvertisementInfo.isSetImpressionInfos)) {
            val eventType = ue.getEventType.getType
            val family = if (eventType == 200) "u"
            else if (eventType == 115) "s"
            else if (eventType == 99) "c"
            else throw new Exception(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

            if (eventType == 200 || eventType == 115) {
              // .u or .s flatten impressionInfos
              val impressionInfos = ue.getAdvertisementInfo.getImpressionInfos
              impressionInfos.map(impressionInfo => {
                val showRequestId = impressionInfo.getShowRequestId

                val put = new Put(Bytes.toBytes(showRequestId.toString.hashCode))
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
                (new ImmutableBytesWritable, put)
              })
            } else {
              // .c
              val impressionInfo = ue.getAdvertisementInfo.getImpressionInfo
              val showRequestId = impressionInfo.getShowRequestId

              val put = new Put(Bytes.toBytes(showRequestId.toString.hashCode))
              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(ue.getLogId), ue.getEventTime, raw)
              Seq((new ImmutableBytesWritable, put))
            }
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

    Runtime.getRuntime().addShutdownHook {
      new Thread() {
        override def run() {
          ZkUtils.deletePath(zkClient, zkRootPath)
          zkClient.close()
        }
      }
    }
    ssc.addStreamingListener(new StreamingJobMonitor(ssc, zkClient))
    ssc.start()
    ssc.awaitTermination()
  }

  def toPut(pair: (Long, String)) = {
    // showRequestId's hashcode used for rowkey for random distributed
    val rowkey = pair._1.toString.hashCode

    val raw = pair._2
    val event = LogUtils.ThriftBase64decoder(raw, classOf[UnitedEvent])
    val logId = event.getLogId
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val put = new Put(Bytes.toBytes(rowkey))

    if (eventType == 200) {
      // e.u
      put.addColumn(Bytes.toBytes("u"), Bytes.toBytes(logId), ts, Bytes.toBytes(raw))
    } else if (eventType == 115) {
      // e.s
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes(logId), ts, Bytes.toBytes(raw))
    } else if (eventType == 99) {
      // e.c
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes(logId), ts, Bytes.toBytes(raw))
    }

    (new ImmutableBytesWritable, put)
  }

}

