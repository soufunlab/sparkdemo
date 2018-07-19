package com.demo.etls

import java.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Hello world!
  *
  */
object KafkaToKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectStream").setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
    val kafkaParams = Map(
      "zookeeper.connect" -> Config.zkQuorum,
      "group.id" -> Config.groupId,
      "metadata.broker.list" -> Config.brokerList,
      "auto.offset.reset" -> "smallest")
    val topics = Set(Config.topic)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    directKafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        var props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.brokerList)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)
        partition.filter(item => item._2.split("∫").length == 8).foreach(item => {
          val message = new ProducerRecord[String, String](Config.produceTopic, null, item._2)
          producer.send(message)
        })
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
