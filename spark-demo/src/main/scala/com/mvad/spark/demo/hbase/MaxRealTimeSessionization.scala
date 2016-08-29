package com.mvad.spark.demo.hbase

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
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import org.slf4j.LoggerFactory

/**
  * Usage:
  * spark-submit --class \
  * com.mvad.spark.demo.hbase.MaxRealTimeSessionization e.u.6,e.s.6.e.c.6 5 maxsession
  */
object MaxRealTimeSessionization {
  val log = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: MaxRealTimeSessionization <topics> <batchInteval> <htablename>")
      System.exit(1)
    }

    val Array(topics, batchInteval, htablename) = args
    val sparkConf = new SparkConf().setAppName("MaxRealTimeSessionization")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchInteval.toInt))
    ssc.checkpoint("checkpoint-MaxRealTimeSessionization")

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
    lines.foreachRDD((rdd: RDD[String], time: Time) => {

      try {
        val events = rdd.map(base64str =>
          LogUtils.ThriftBase64decoder(base64str, classOf[com.mediav.data.log.unitedlog.UnitedEvent]))
        //          .flatMap(unitedEvent => UnitedEventTransformer.transform2DSPEvent(unitedEvent).toArray())
        //          .filter(e => e.isInstanceOf[DSPEvent]).map(e => e.asInstanceOf[DSPEvent])
        val raw = events.map(toPut)
        val index = events.map(toIndex)

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
        conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
        conf.set(TableOutputFormat.OUTPUT_TABLE, htablename)
        raw.saveAsNewAPIHadoopDataset(conf)

        conf.set(TableOutputFormat.OUTPUT_TABLE, htablename + "-index-mvid")
        index.saveAsNewAPIHadoopDataset(conf)
      } catch {
        case ex: NullPointerException => log.warn("NPE: ", ex)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def toPut(event: UnitedEvent) = {

    val mvid = if (event.isSetLinkedIds && event.getLinkedIds.isSetMvid) event.getLinkedIds.getMvid else "UNKNOWN_MVID"
    val bidId = if (event.isSetAdvertisementInfo && event.getAdvertisementInfo.isSetExchangeBidId) event.getAdvertisementInfo.getExchangeBidId else "UNKNOWN_BIDID"
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val serializer = new TSerializer(new TCompactProtocol.Factory())
    val put = new Put(Bytes.toBytes(bidId))

    if (eventType == 200) {
      // e.u
      put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("raw"), ts, serializer.serialize(event))
    } else if (eventType == 115) {
      // e.s
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("raw"), ts, serializer.serialize(event))
    } else if (eventType == 99) {
      // e.c
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("raw"), ts, serializer.serialize(event))
    }

    (new ImmutableBytesWritable, put)
  }

  def toIndex(event: UnitedEvent) = {

    val mvid = if (event.isSetLinkedIds && event.getLinkedIds.isSetMvid) event.getLinkedIds.getMvid else "UNKNOWN_MVID"
    val bidId = if (event.isSetAdvertisementInfo && event.getAdvertisementInfo.isSetExchangeBidId) event.getAdvertisementInfo.getExchangeBidId else "UNKNOWN_BIDID"
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val serializer = new TSerializer(new TCompactProtocol.Factory())
    val put = new Put(Bytes.toBytes(mvid))

    if (eventType == 200) {
      // e.u
      put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("id"), ts, Bytes.toBytes(bidId))
    } else if (eventType == 115) {
      // e.s
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("id"), ts, Bytes.toBytes(bidId))
    } else if (eventType == 99) {
      // e.c
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("id"), ts, Bytes.toBytes(bidId))
    }

    (new ImmutableBytesWritable, put)
  }

}

