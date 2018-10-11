package com.demo.compute.jobs

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object DeepDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("deep-perday")
    //          .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new HiveContext(sc)

    import sqlCtx.implicits._
    import sqlCtx.sql

    val df = sql(
      s"""
         |select count(distinct(pageurl)) from source_data where date=${date}
         |group by traceid
      """.stripMargin).as[Long]

    val countRdd = df.rdd.map(r => (r, 1)).reduceByKey(_ + _).persist()

    val deep_1 = deep_x(countRdd, 1)
    val deep_2 = deep_x(countRdd, 2)
    val deep_3 = deep_x(countRdd, 3)
    val deep_4 = deep_x(countRdd, 4)
    val deep_5 = deep_x(countRdd, 5)
    val deep_6_10 = deep_x(countRdd, 6, 10)
    val deep_11_50 = deep_x(countRdd, 11, 50)
    val deep_50 = deep_x(countRdd, 50, Int.MaxValue)

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:deep_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("1"), Bytes.toBytes(deep_1.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("2"), Bytes.toBytes(deep_2.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("3"), Bytes.toBytes(deep_3.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("4"), Bytes.toBytes(deep_4.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("5"), Bytes.toBytes(deep_5.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("6_10"), Bytes.toBytes(deep_6_10.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("11_50"), Bytes.toBytes(deep_11_50.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("50"), Bytes.toBytes(deep_50.toString))
      table.put(put)
    } finally {
      table.close()
    }

  }


  def deep_x(rdd: RDD[(Long, Int)], value: Int): Int = {
    deep_x(rdd, value, value)
  }

  def deep_x(rdd: RDD[(Long, Int)], start: Int, end: Int) = {
    var crdd = rdd.filter(r => r._1 >= start && r._1 <= end).map(e => e._2)
    if (!crdd.isEmpty())
      crdd.reduce(_ + _)
    else 0
  }


}
