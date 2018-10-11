package com.demo.etls

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-19 16:56 by 李浩（lihao@cloud-young.com）创建
  */
object Kafka2hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafka2hive")
//                  .setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
    val hiveContext = new HiveContext(ssc.sparkContext)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    val kafkaParams = Map(
      "zookeeper.connect" -> Config.zkQuorum,
      "group.id" -> Config.groupId,
      "metadata.broker.list" -> Config.brokerList,
      "auto.offset.reset" -> "largest")
    val topics = Set(Config.topic)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    import hiveContext.implicits._
    import hiveContext.sql
    directKafkaStream.foreachRDD(rdd => {
      println("running")
      if (!rdd.isEmpty()) {
        println(rdd.count())
        rdd.map(i => i._2.split("\t")).map {
          case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
          => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os, "test")
        }.toDF.registerTempTable("temp")
//        sql("select * from temp").show()
        sql(s"insert into table source_data partition(date='${date}') select * from temp")
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  case class Entity(val time: String, val openid: String, val traceid: String
                    , val sourceurl: String, val pageurl: String, val staytime: String
                    , val province: String, val city: String, val event: String
                    , val device: String, val os: String, val value: String)


  def date = {
    val sf = new SimpleDateFormat("yyyyMMdd")
    sf.format(new Date())
  }

}
