package com.mvad.spark.demo.streaming

import java.util.Base64

import com.mediav.data.log.LogUtils
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
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
    sparkConf.set("spark.scheduler.mode", "FAIR")

    val sc = new SparkContext(sparkConf)
    sc.setLocalProperty("spark.scheduler.pool", "etl")

    val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
    ssc.checkpoint(s"checkpoint-${appName}")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
    conf.setLong("hbase.client.write.buffer", 3 * 1024 * 1024)
    val hbaseContext = new HBaseContext(sc, conf)

    /* using receiver mode */
    //    val topicMap = topics.split(",").map((_, numThreadsPerReceiver.toInt)).toMap
    //    val kafkaStreams = (1 to numReceivers.toInt).map {
    //      i => KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //    }
    //    val unifiedStream = ssc.union(kafkaStreams)
    //    val lines = unifiedStream.map(_._2)

    /* using direct mode */
    val topicsSet = topics.split(",").toSet
    val brokers = "kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    log.info(s"Initializing kafka Receiver, topics: ${topics} ; brokers: ${brokers} ; kafkaParams: ${kafkaParams.mkString(",")} ")

    val kafkaCluster = new KafkaCluster(kafkaParams)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    /* update Zk Offsets */
    var offsetRanges = Array[OffsetRange]()
    val stream = kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (offsets <- offsetRanges) {
        val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
        val o = kafkaCluster.setConsumerOffsets(appName, Map((topicAndPartition, offsets.fromOffset)))
        if (o.isLeft) {
          println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
        }
      }
      rdd
    }

    // convert to put
    val puts = stream.map(record => record._2).flatMap(line => {

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
        (new ImmutableBytesWritable, put)
      })
      putRecords
    })

    hbaseContext.streamBulkPut[(ImmutableBytesWritable, Put)](puts,TableName.valueOf(htablename), putRecord => putRecord._2)

//    saveToHBase(rdd, htablename)


    ssc.addStreamingListener(new StreamingJobMonitor(sparkConf))
    ssc.start()
    ssc.awaitTermination()
  }

  def saveToHBase(rdd: DStream[(String, String)], htablename: String) = {
    rdd.map(_._2).foreachRDD((rdd: RDD[String], time: Time) => {

      /* convert to Put and save to HBase */
      try {
        val puts = rdd.flatMap(line => {
          val raw = Base64.getDecoder.decode(line)
          val ue = LogUtils.thriftBinarydecoder(raw, classOf[com.mediav.data.log.unitedlog.UnitedEvent])
          if (ue != null && ue.isSetAdvertisementInfo && (ue.getAdvertisementInfo.isSetImpressionInfo || ue.getAdvertisementInfo.isSetImpressionInfos)) {
            val eventType = ue.getEventType.getType
            val family = if (eventType == 200) "u"
            else if (eventType == 115) "s"
            else if (eventType == 99) "c"
            else throw new SparkException(s"not supported eventType: ${eventType}, only support topic .u/.s/.c")

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

    }

    )


  }

}

