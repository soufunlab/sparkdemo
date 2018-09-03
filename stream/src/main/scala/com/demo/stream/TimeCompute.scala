package com.demo.stream

import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-19 16:56 by 李浩（lihao@cloud-young.com）创建
  */
object TimeCompute {

  val event_register = "register"
  val event_load = "load"
  val event_view = "view"
  val event_exit = "exit"
  val event_error = "error"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectStream")
    //      .setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
    val kafkaParams = Map(
      "zookeeper.connect" -> Config.zkQuorum,
      "group.id" -> Config.groupId,
      "metadata.broker.list" -> Config.brokerList,
      "auto.offset.reset" -> "largest")
    val topics = Set(Config.topic)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    directKafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val time = this.getHour()
        newAdd(rdd, time)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 新增用户
    */
  def newAdd(rdd: RDD[(String, String)], hour: String): Unit = {
    rdd.map(i => i._2).map(i => i.split("\t")).map { i =>
      i match {
        case Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
        => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      }
    }.filter(e => e.event == event_register)
  }

  def getHour() = {
    val calendar = Calendar.getInstance()
    var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val min = calendar.get(Calendar.MINUTE)
    if (min <= 30) {
      simpleDateFormat.format(calendar.getTime)
    } else {
      calendar.add(Calendar.HOUR, 1)
      simpleDateFormat.format(calendar.getTime)
    }
  }

  case class Entity(val time: String, val openid: String, val traceid: String, val sourceurl: String, val pageurl: String, val staytime: String, val province: String, val city: String, val event: String, val device: String, val os: String)

}
