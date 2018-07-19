package com.demo.etls

import java.net.URI
import java.util

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-19 16:56 by 李浩（lihao@cloud-young.com）创建
  */
object KafkaToHdfs {
  val conf = new SparkConf().setAppName("KafkaDirectStream").setMaster("local[1]")
  val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
  val kafkaParams = Map(
    "zookeeper.connect" -> Config.zkQuorum,
    "group.id" -> Config.groupId,
    "metadata.broker.list" -> Config.brokerList,
    "auto.offset.reset" -> "smallest")
  val topics = Set(Config.topic)

  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  val config = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://ubuntu2:9000"),config);
  var output = "test"

  directKafkaStream.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val path = hdfsPath;
      if (!fs.exists(path)){
        val fin = fs.create(path)
        rdd.foreach(item => {
          fin.writeUTF(item + "\n")
        })
      }else{
        val fin = fs.append(path)
        rdd.foreach(item => {
          fin.writeUTF(item + "\n")
        })
      }
    }
  })

  ssc.start()
  ssc.awaitTermination()

  def hdfsPath = {
    import java.util.Calendar
    new Path(output + "/" + Calendar.getInstance.get(Calendar.YEAR) + "-" + (Calendar.getInstance.get(Calendar.MONTH) + 1) + "-" + Calendar.getInstance.get(Calendar.DATE) + "/" + Calendar.getInstance.get(Calendar.YEAR) + "-" + (Calendar.getInstance.get(Calendar.MONTH) + 1) + "-" + Calendar.getInstance.get(Calendar.DATE) + ".txt")
  }
}
