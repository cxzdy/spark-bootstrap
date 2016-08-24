package com.mvad.spark.demo.es

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.mediav.data.log.LogUtils
import com.mediav.ods.UnitedEventTransformer
import com.mediav.ods.dsp.DSPEvent
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TSimpleJSONProtocol
import org.elasticsearch.spark._

/**
  * Usage:
  * `$ bin/spark-submit --class \
  * com.mvad.spark.demo.es d.c.6,d.c.6.m 5 dspevents`
  */
object KafkaDSPEvent2ESIndexer {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Usage: KafkaDSPEvent2ESIndexer <topics> <batchInteval> <esIndexName>")
      System.exit(1)
    }

    val Array(topics, batchInteval, esIndexName) = args
    val sparkConf = new SparkConf().setAppName("Kafka2ESIndexer")
    sparkConf.set("es.nodes", "es1ss.prod.mediav.com,es2ss.prod.mediav.com," +
      "es3ss.prod.mediav.com,es4ss.prod.mediav.com,es5ss.prod.mediav.com")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInteval.toInt))
    ssc.checkpoint("checkpoint-KafkaDSPEvent2ESIndexer")

    // Kafka configurations
    val topicsSet = topics.split(",").toSet
    val brokers = "kf4ss.prod.mediav.com:9092,kf5ss.prod.mediav.com:9092,kf6ss.prod.mediav.com:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    val lines = kafkaStream.map(_._2)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      // Convert RDD[String] to RDD[case class] to DataFrame
      val events = rdd.map(base64str =>
        LogUtils.ThriftBase64decoder(base64str, classOf[com.mediav.data.log.unitedlog.UnitedEvent]))
        .flatMap(unitedEvent => UnitedEventTransformer.transform2DSPEvent(unitedEvent).toArray())
        .filter(e => e.isInstanceOf[DSPEvent]).map(e => toJson(e.asInstanceOf[DSPEvent]))

      val day = new SimpleDateFormat("yyyy.MM.dd").format(new Date(time.milliseconds))

      events.saveJsonToEs(esIndexName + "-" + day + "/{logInfo.eventType.type}",
        Map("es.mapping.id" -> "logInfo.logId", "es.mapping.timestamp" -> "timestamp"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def toJson(dspEvent: DSPEvent): String = {
    val serializer = new TSerializer(new TSimpleJSONProtocol.Factory())
    val jsonString = serializer.toString(dspEvent)

    // add a timestamp field
    val mapper = new ObjectMapper()
    val jsonNode = mapper.readTree(jsonString)

    val eventTime = jsonNode.get("logInfo").get("eventTime").asLong()
    val tz = TimeZone.getTimeZone("UTC");
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    df.setTimeZone(tz)
    val timestring = df.format(new Date(eventTime))

    jsonNode.asInstanceOf[ObjectNode].put("timestamp", timestring)
    if (jsonNode.asInstanceOf[ObjectNode].get("contextInfo").has("params")) {
      jsonNode.asInstanceOf[ObjectNode].get("contextInfo").asInstanceOf[ObjectNode].remove("params")
    }

    jsonNode.toString
  }


}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

}
