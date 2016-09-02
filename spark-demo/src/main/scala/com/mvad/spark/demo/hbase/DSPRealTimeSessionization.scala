package com.mvad.spark.demo.hbase

import java.util

import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog.UnitedEvent
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TCompactProtocol
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
  * Usage:
  * spark-submit --class \
  * com.mvad.spark.demo.hbase.DSPRealTimeSessionization d.b.6,d.b.6.m,d.c.6,d.c.6.m 5 dspsession
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
    val impressions = lines.map(base64str =>
      LogUtils.ThriftBase64decoder(base64str, classOf[com.mediav.data.log.unitedlog.UnitedEvent]))
      .flatMap(toImpressions)

    //    impressions.foreachRDD((rdd: RDD[(Long, UnitedEvent)], time: Time) => {
    //      rdd.foreachPartition(partitionOfRecords => {
    //        partitionOfRecords.foreach(pair => {
    //
    //        }
    //      })
    //      )
    //    }

    //    impressions.foreachRDD((rdd: RDD[(Long, UnitedEvent)], time: Time) => {
    //
    //      rdd.foreachPartition(partitionOfRecords => {
    //        val conf = HBaseConfiguration.create()
    //        conf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
    //
    //        val conn = ConnectionFactory.createConnection(conf)
    //        val table = new HTable(conf, TableName.valueOf(htablename))
    ////        table.setAutoFlush(false, false)
    //        table.setWriteBufferSize(3 * 1024 * 1024)
    //
    //        partitionOfRecords.foreach(pair => {
    //          val put = toPut(pair)
    //          table.put(put._2)
    //        })
    //        table.flushCommits()
    //        table.close()
    //      })
    //    })


    impressions.foreachRDD((rdd: RDD[(Long, UnitedEvent)], time: Time) => {
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

    //    impressions.foreachRDD((rdd: RDD[(Long, UnitedEvent)], time: Time) => {
    //      try {
    //        val index = rdd.map(toIndex)
    //
    //        val indexconf = HBaseConfiguration.create()
    //        indexconf.set("hbase.zookeeper.quorum", "nn7ss.prod.mediav.com,nn8ss.prod.mediav.com,nn9ss.prod.mediav.com")
    //        indexconf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
    //        indexconf.set(TableOutputFormat.OUTPUT_TABLE, htablename + "-index-mvid")
    //        index.saveAsNewAPIHadoopDataset(indexconf)
    //      } catch {
    //        case ex: NullPointerException => log.warn("NPE: ", ex)
    //      }
    //    })
    ssc.start()
    ssc.awaitTermination()
  }

  def toImpressions(event: UnitedEvent) = {

    val impList = new util.ArrayList[(Long, UnitedEvent)]()

    val eventType = event.getEventType.getType
    if (eventType == 200 || eventType == 115) {
      // d.u or d.s
      val impressionInfos = event.getAdvertisementInfo.getImpressionInfos
      if (impressionInfos != null) {
        for (impressionInfo <- impressionInfos) {
          impList.add((impressionInfo.getShowRequestId, event))
        }
      }
    } else if (eventType == 99) {
      // d.c
      val impressionInfo = event.getAdvertisementInfo.getImpressionInfo
      impList.add((impressionInfo.getShowRequestId, event))
    }
    impList
  }

  def toPut(pair: (Long, UnitedEvent)) = {

    val showRequestId = pair._1.toString.hashCode
    val event = pair._2
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val serializer = new TSerializer(new TCompactProtocol.Factory())
    val put = new Put(Bytes.toBytes(showRequestId))

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

  def toIndex(pair: (Long, UnitedEvent)) = {

    val showRequestId = pair._1
    val event = pair._2
    val mvid = if (event.isSetLinkedIds && event.getLinkedIds.isSetMvid) event.getLinkedIds.getMvid else "UNKNOWN_MVID"
    val eventType = event.getEventType.getType
    val ts = event.getEventTime

    val serializer = new TSerializer(new TCompactProtocol.Factory())
    val put = new Put(Bytes.toBytes(mvid))

    if (eventType == 200) {
      // e.u
      put.addColumn(Bytes.toBytes("u"), Bytes.toBytes("id"), ts, Bytes.toBytes(showRequestId))
    } else if (eventType == 115) {
      // e.s
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("id"), ts, Bytes.toBytes(showRequestId))
    } else if (eventType == 99) {
      // e.c
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("id"), ts, Bytes.toBytes(showRequestId))
    }

    (new ImmutableBytesWritable, put)
  }

  /*
  def toPut(event: DSPEvent) = {

    val showId = if (event.isSetShow && event.getShow.isSetShowId) event.getShow.getShowId else 0f

    val put = new Put(Bytes.toBytes(showId))

    put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("logId"), Bytes.toBytes(event.getLogInfo.getLogId))
    put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("seqId"), Bytes.toBytes(event.getLogInfo.getSeqId))
    put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("type"), Bytes.toBytes(event.getLogInfo.getEventType.getType.toString))
    if (event.getLogInfo.isSetLogFlags){
      put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("logFlags"), Bytes.toBytes(event.getLogInfo.getLogFlags.mkString(",")))
    }
    if (event.getLogInfo.isSetParamFlags){
      put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("paramFlags"), Bytes.toBytes(event.getLogInfo.getParamFlags.mkString(",")))
    }
    if (event.getLogInfo.isSetErrorFlag){
      put.addColumn(Bytes.toBytes("log"), Bytes.toBytes("errorFlags"), Bytes.toBytes(event.getLogInfo.getErrorFlag.mkString(",")))
    }

    val mvid = if (event.getUser.isSetMvid) event.getUser.getMvid else "UNKNOWN"
    put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mvid"), Bytes.toBytes(mvid))

    if (event.getUser.isSetMdid) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mdid"), Bytes.toBytes(event.getUser.getMdid))
    }
    if (event.getUser.isSetIdfa) {
      if (event.getUser.getIdfa.isSetId) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("idfa"), Bytes.toBytes(event.getUser.getIdfa.getId))
      }
    }
    if (event.getUser.isSetImei) {
      if (event.getUser.getImei.isSetId) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("imei"), Bytes.toBytes(event.getUser.getImei.getId))
      }
    }
    if (event.getUser.isSetAndroidId) {
      if (event.getUser.getAndroidId.isSetId) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("androidId"), Bytes.toBytes(event.getUser.getAndroidId.getId))
      }
    }
    if (event.getUser.isSetMac) {
      if (event.getUser.getMac.isSetId) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mac"), Bytes.toBytes(event.getUser.getMac.getId))
      }
    }
    if (event.getUser.isSetM2id) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("m2id"), Bytes.toBytes(event.getUser.getM2id))
    }
    if (event.isSetUserInfo) {
      if (event.getUserInfo.isSetUserLabels) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userlabels"), Bytes.toBytes(event.getUserInfo.getUserLabels.mkString(",")))
      }
    }
    if (event.getContextInfo.isSetDeviceInfo) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("deviceInfo"), Bytes.toBytes(event.getContextInfo.getDeviceInfo.toString))
    }
    if (event.getContextInfo.isSetIp) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ip"), Bytes.toBytes(event.getContextInfo.getIp.mkString(".")))
    }
    if (event.getContextInfo.isSetGeoInfo) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("geoInfo"), Bytes.toBytes(event.getContextInfo.getGeoInfo.toString.substring(7)))
    }
    if (event.getContextInfo.isSetUserAgent) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ua"), Bytes.toBytes(event.getContextInfo.getUserAgent))
    }
    if (event.getContextInfo.isSetLanguage) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("language"), Bytes.toBytes(event.getContextInfo.getLanguage.toString))
    }
    if (event.getContextInfo.isSetAcceptLanguage) {
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("acceptLanguage"), Bytes.toBytes(event.getContextInfo.getAcceptLanguage))
    }
    if (event.getContextInfo.isSetMobileInfo) {
      if (event.getContextInfo.getMobileInfo.isSetDeviceType) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("deviceType"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getDeviceType.toString))
      }
      if (event.getContextInfo.getMobileInfo.isSetAppInfo) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("appInfo"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getAppInfo.toString.substring(7)))
      }
      if (event.getContextInfo.getMobileInfo.isSetSdkInfo) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sdkInfo"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getSdkInfo.toString.substring(7)))
      }
      if (event.getContextInfo.getMobileInfo.isSetLongitude) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("longitude"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getLongitude))
      }
      if (event.getContextInfo.getMobileInfo.isSetLatitude) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("latitude"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getLatitude))
      }
      if (event.getContextInfo.getMobileInfo.isSetAdType) {
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mobileAdType"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getAdType.toString))
      }
    }
    if (event.isSetUrlInfo) {
      if (event.getUrlInfo.isSetUri) {
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("uri"), Bytes.toBytes(event.getUrlInfo.getUri))
      }
      if (event.getUrlInfo.isSetPageReferralUrl) {
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("pageReferralUrl"), Bytes.toBytes(event.getUrlInfo.getPageReferralUrl))
      }
    }
    if (event.isSetAdSlotInfo) {
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("adslotInfo"), Bytes.toBytes(event.getAdSlotInfo.toString))
    }
    if (event.isSetCreativeInfo) {
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("creativeInfo"), Bytes.toBytes(event.getCreativeInfo.toString))
    }
    if (event.isSetCreativeLabel) {
      if (event.getCreativeLabel.isSetBreedIds) {
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("breedIds"), Bytes.toBytes(event.getCreativeLabel.getBreedIds.mkString(",")))
      }
      if (event.getCreativeLabel.isSetFrontLabels) {
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("frontLabels"), Bytes.toBytes(event.getCreativeLabel.getFrontLabels.mkString(",")))
      }
    }
    if (event.isSetBid) {
      put.addColumn(Bytes.toBytes("b"), Bytes.toBytes("bid"), Bytes.toBytes(event.getBid.toString))
    }
    if (event.isSetShow) {
      put.addColumn(Bytes.toBytes("s"), Bytes.toBytes("show"), Bytes.toBytes(event.getShow.toString))
    }
    if (event.isSetClick) {
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("click"), Bytes.toBytes(event.getClick.toString))
    }

    (new ImmutableBytesWritable, put)
  }
  */
}

