package com.demo.spstream

import java.net.URI
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.demo.spstream.utils.{Config, KafkaManager, Utils}
import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-19 16:56 by 李浩（lihao@cloud-young.com）创建
  */
object TimeCompute {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("time-compute")
//      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
    val kafkaParams = Map(
      "zookeeper.connect" -> Config.zkQuorum,
      "group.id" -> Config.groupId,
      "metadata.broker.list" -> Config.brokerList,
      "auto.offset.reset" -> "largest",
      "spark.streaming.kafka.maxRatePerPartition" -> "1000")
    val topics = Set(Config.topic)
    val km = new KafkaManager(kafkaParams)

    val directKafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    directKafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val time = Utils.getTime()
        RegisterJob.execute(rdd, time)
        UserStartupJob.execute(rdd, time)
        StartupsJob.execute(rdd, time)
        AvgTimeLengthJob.execute(rdd, time)
        DayUserAvgStartupsJob.execute(ssc.sparkContext, time)
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
