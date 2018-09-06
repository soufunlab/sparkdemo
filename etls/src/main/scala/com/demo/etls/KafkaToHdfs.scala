package com.demo.etls

import java.net.URI
import java.util

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
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
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectStream")
//          .setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(Config.timeInterval.toInt))
    val kafkaParams = Map(
      "zookeeper.connect" -> Config.zkQuorum,
      "group.id" -> Config.groupId,
      "metadata.broker.list" -> Config.brokerList,
      "auto.offset.reset" -> "largest")
    val topics = Set(Config.topic)

    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val config = new Configuration()
    import org.apache.hadoop.fs.FileSystem
    config.set("dfs.nameservices", "nameservice1")
    config.set("dfs.ha.namenodes.nameservice1", "namenode46,namenode64")
    config.set("dfs.namenode.rpc-address.nameservice1.namenode46", "master:8020")
    config.set("dfs.namenode.rpc-address.nameservice1.namenode64", "slave1:8020")
    config.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    config.set("fs.defaultFS", "hdfs://nameservice1")
    val fs = FileSystem.get(new URI("hdfs://nameservice1"), config, "root");
    //    val fs = FileSystem.get(new URI("hdfs://slave1:8020/"), config);
    var output = "test"

    var i = 0
    directKafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        print(i = i + 1)
        val path = hdfsPath;
        val pathUrl = path.toUri

        var outputStream: FSDataOutputStream = null
        if (!fs.exists(path)) {
          outputStream = fs.create(path)
        } else {
          outputStream = fs.append(path);
        }

        try {
          var count = rdd.count();
          println(count)
          rdd.collect().foreach(item => {
            val line = item._2 + "\n"
            outputStream.write(line.getBytes, 0, line.getBytes.length)
          })
        }
        finally {
          outputStream.close()
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
}
