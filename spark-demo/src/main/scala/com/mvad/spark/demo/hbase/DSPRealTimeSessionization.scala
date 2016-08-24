package com.mvad.spark.demo.hbase

import com.mediav.data.log.LogUtils
import com.mediav.ods.UnitedEventTransformer
import com.mediav.ods.dsp.DSPEvent
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

import scala.collection.JavaConversions._
/**
  * Usage:
  * spark-submit --class \
  * com.mvad.spark.demo.hbase.DSPRealTimeSessionization d.b.6,d.b.6.m 5 dspsession
  */
object DSPRealTimeSessionization {
  def main(args: Array[String]) {
    if (args.length != 6) {
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
    lines.foreachRDD((rdd: RDD[String], time: Time) => {

      val events = rdd.map(base64str =>
        LogUtils.ThriftBase64decoder(base64str, classOf[com.mediav.data.log.unitedlog.UnitedEvent]))
        .flatMap(unitedEvent => UnitedEventTransformer.transform2DSPEvent(unitedEvent).toArray())
        .filter(e => e.isInstanceOf[DSPEvent]).map(e => e.asInstanceOf[DSPEvent])
      val puts = events.map(toPut)

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "nn4ss.prod.mediav.com,nn5ss.prod.mediav.com,nn6ss.prod.mediav.com")
      conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat")
      conf.set(TableOutputFormat.OUTPUT_TABLE, htablename)

      puts.saveAsNewAPIHadoopDataset(conf)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def toPut(event: DSPEvent) = {
    val key = event.getUser.getMvid.concat("_").concat(event.getShow.getImpressionId.toString)
    val put = new Put(Bytes.toBytes(key))

    put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mvid"), Bytes.toBytes(event.getUser.getMvid))

    if (event.getUser.isSetMdid){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mdid"), Bytes.toBytes(event.getUser.getMdid))
    }
    if (event.getUser.isSetIdfa){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("idfa"), Bytes.toBytes(event.getUser.getIdfa.getId))
    }
    if (event.getUser.isSetImei){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("imei"), Bytes.toBytes(event.getUser.getImei.getId))
    }
    if (event.getUser.isSetAndroidId){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("androidId"), Bytes.toBytes(event.getUser.getAndroidId.getId))
    }
    if (event.getUser.isSetMac){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mac"), Bytes.toBytes(event.getUser.getMac.getId))
    }
    if (event.getUser.isSetM2id){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("m2id"), Bytes.toBytes(event.getUser.getM2id))
    }
    if (event.isSetUserInfo){
      if (event.getUserInfo.isSetUserLabels){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("userlabels"), Bytes.toBytes(event.getUserInfo.getUserLabels.mkString(",")))
      }
    }
    if (event.getContextInfo.isSetDeviceInfo){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("deviceInfo"), Bytes.toBytes(event.getContextInfo.getDeviceInfo.toString))
    }
    if (event.getContextInfo.isSetIp){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ip"), Bytes.toBytes(event.getContextInfo.getIp.mkString(".")))
    }
    if (event.getContextInfo.isSetGeoInfo){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("geoInfo"), Bytes.toBytes(event.getContextInfo.getGeoInfo.toString.substring(7)))
    }
    if (event.getContextInfo.isSetUserAgent){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("ua"), Bytes.toBytes(event.getContextInfo.getUserAgent))
    }
    if (event.getContextInfo.isSetLanguage){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("language"), Bytes.toBytes(event.getContextInfo.getLanguage.toString))
    }
    if (event.getContextInfo.isSetAcceptLanguage){
      put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("acceptLanguage"), Bytes.toBytes(event.getContextInfo.getAcceptLanguage))
    }
    if (event.getContextInfo.isSetMobileInfo){
      if (event.getContextInfo.getMobileInfo.isSetDeviceType){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("deviceType"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getDeviceType.toString))
      }
      if (event.getContextInfo.getMobileInfo.isSetAppInfo){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("appInfo"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getAppInfo.toString.substring(7)))
      }
      if (event.getContextInfo.getMobileInfo.isSetSdkInfo){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("sdkInfo"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getSdkInfo.toString.substring(7)))
      }
      if (event.getContextInfo.getMobileInfo.isSetLongitude){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("longitude"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getLongitude))
      }
      if (event.getContextInfo.getMobileInfo.isSetLatitude){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("latitude"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getLatitude))
      }
      if (event.getContextInfo.getMobileInfo.isSetAdType){
        put.addColumn(Bytes.toBytes("user"), Bytes.toBytes("mobileAdType"), Bytes.toBytes(event.getContextInfo.getMobileInfo.getAdType.toString))
      }
    }
    if (event.isSetUrlInfo){
      if (event.getUrlInfo.isSetUri){
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("uri"), Bytes.toBytes(event.getUrlInfo.getUri))
      }
      if (event.getUrlInfo.isSetPageReferralUrl){
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("pageReferralUrl"), Bytes.toBytes(event.getUrlInfo.getPageReferralUrl))
      }
    }
    if (event.isSetAdSlotInfo){
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("adslotInfo"), Bytes.toBytes(event.getAdSlotInfo.toString))
    }
    if (event.isSetCreativeInfo){
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("creativeInfo"), Bytes.toBytes(event.getCreativeInfo.toString))
    }
    if (event.isSetCreativeLabel){
      if (event.getCreativeLabel.isSetBreedIds){
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("breedIds"), Bytes.toBytes(event.getCreativeLabel.getBreedIds.mkString(",")))
      }
      if (event.getCreativeLabel.isSetFrontLabels){
        put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("frontLabels"), Bytes.toBytes(event.getCreativeLabel.getFrontLabels.mkString(",")))
      }
    }
    if (event.isSetBid){
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("bid"), Bytes.toBytes(event.getBid.toString))
    }
    if (event.isSetShow){
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("show"), Bytes.toBytes(event.getShow.toString))
    }
    if (event.isSetClick){
      put.addColumn(Bytes.toBytes("tx"), Bytes.toBytes("click"), Bytes.toBytes(event.getShow.toString))
    }

    (new ImmutableBytesWritable, put)
  }
}

