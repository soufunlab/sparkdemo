package com.demo.compute.jobs

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object StartupsDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("startups-perday")
//      .setMaster("local")
    val sc = new SparkContext(conf)

    Utils.setHadoopConf(sc.hadoopConfiguration)

    val path = s"hdfs://nameservice1/user/root/test/${Utils.dfs_date(date)}/${Utils.dfs_date(date)}.txt"
    val hadoopRdd = sc.textFile(path).map(i => i.split("\t")).map(i => i match {
      case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      => LogObj(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    })

    val countRdd = hadoopRdd.filter(e => e.event == Utils.event_load)
      .map(e => (e.openid, 1)).reduceByKey(_ + _)
      .map(e => (e._2, 1)).reduceByKey(_ + _)

    val deep_1_2 = deep_x(countRdd, 1, 2)
    val deep_3_5 = deep_x(countRdd, 3, 5)
    val deep_6_9 = deep_x(countRdd, 6, 9)
    val deep_10_19 = deep_x(countRdd, 10, 19)
    val deep_20_49 = deep_x(countRdd, 20, 49)
    val deep_50 = deep_x(countRdd, 50, Int.MaxValue)

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:startups_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("1_2"), Bytes.toBytes(deep_1_2.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("3_5"), Bytes.toBytes(deep_3_5.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("6_9"), Bytes.toBytes(deep_6_9.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("10_19"), Bytes.toBytes(deep_10_19.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("20_49"), Bytes.toBytes(deep_20_49.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("50"), Bytes.toBytes(deep_50.toString))
      table.put(put)
    } finally {
      table.close()
    }

  }

  def deep_x(rdd: RDD[(Int, Int)], start: Int, end: Int) = {
    val countRdd = rdd.filter(r => r._1 >= start && r._1 <= end).map(e => e._2)
    if (!countRdd.isEmpty()) {
      countRdd.reduce(_ + _)
    } else 0
  }


}
