/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mvad.spark.demo.es

import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.mediav.data.log.LogUtils
import com.mediav.data.log.unitedlog._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.thrift.{TSerializer, TBase}
import org.apache.thrift.protocol.TSimpleJSONProtocol
import org.elasticsearch.spark.rdd.EsSpark

/**
 * Usage:
 *    `$ bin/spark-submit --class \
 *      com.mvad.spark.com.mvad.spark.demo.es.Kafka2ESIndexer zk1dg,zk2dg,zk3dg:2191/kafka08 \
 *      my-consumer-group d.b.3,d.b.3.m dsplog/b 1 `
 */
object Kafka2ESIndexer {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.println("Usage: Kafka2ESIndexer <zkQuorum> <group> <topics> " +
        "<esIndexName> <numThreads> <batchInteval>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, esIndexName, numThreads,batchInteval) = args
    val sparkConf = new SparkConf().setAppName("Kafka2ESIndexer")
    sparkConf.set("es.nodes","hadoop.corp.mediav.com,hd5dg.prod.mediav.com," +
      "hd6dg.prod.mediav.com,hd7dg.prod.mediav.com,hd8dg.prod.mediav.com,hd9dg.prod.mediav.com")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInteval.toInt))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val event = rdd.map(base64Decode)
        .map{
          e =>
            val tz = TimeZone.getTimeZone("UTC");
            val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
            df.setTimeZone(tz)
            Event(e.getLogId, e.getEventTime, df.format(new Date(e.getEventTime)),
              e.getEventType.name)
        }
//        .map(e => UnitedEvent(e.getLogId, e.getEventTime, e.getEventType))
//       e.getSeqId, e.getContextInfo, e.getAdvertisementInfo, e.getUserBehavior, e.getLogFlags))

      // Register as table
//      wordsDataFrame.registerTempTable("dsplog")

      // Do word count on table using SQL and print it
//      val wordCountsDataFrame =
//        sqlContext.sql("select advertiserId,sum(fee) as totalfee from dsplog " +
//          "group by advertiserId")
      EsSpark.saveJsonToEs(event, esIndexName)
    })

    ssc.start()
    ssc.awaitTermination()
  }


  def base64Decode(base64Str:String) = {
    val objectMapper = new ObjectMapper();
    val event = LogUtils.ThriftBase64decoder(base64Str,
      classOf[com.mediav.data.log.unitedlog.UnitedEvent])
    event
//    val serializer = new TSerializer(new TSimpleJSONProtocol.Factory())
//    serializer.toString(event)
//    val json = objectMapper.readTree(serializer.toString(event))
//    val on = json.asInstanceOf[ObjectNode];
//    val tz = TimeZone.getTimeZone("UTC");
//    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
//    df.setTimeZone(tz)
//    on.put("eventTimeFormatted", df.format(new Date(json.get("eventTime").asLong())))
//    on.toString
  }

  case class Event(logId:Long, eventTime: Long,eventTimeStr:String,
                        eventType: String)
//    seqId:Long, contextInfo: ContextInfo, advertisementInfo: AdvertisementInfo,
//    userBehavior: UserBehavior, logFlags:Long  )
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {

  @transient  private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

}
