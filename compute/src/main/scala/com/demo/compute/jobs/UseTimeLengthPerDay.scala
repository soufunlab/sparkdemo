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
object UseTimeLengthPerDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("useTimeLength-perday")
      .setMaster("local")
    val sc = new SparkContext(conf)

    Utils.setHadoopConf(sc.hadoopConfiguration)

    val path = s"hdfs://nameservice1/user/root/test/${Utils.dfs_date(date)}/${Utils.dfs_date(date)}.txt"
    val hadoopRdd = sc.textFile(path).map(i => i.split("\t")).map(i => i match {
      case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      => LogObj(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    })

    val useTimeRdd = hadoopRdd.map(e => (e.openid, e.staytime.toInt))
      .reduceByKey(_ + _).map(e => (e._2, 1))


    val deep_0_3 = timeLength_x(useTimeRdd, 0, 3)
    val deep_4_9 = timeLength_x(useTimeRdd, 4, 9)
    val deep_10_29 = timeLength_x(useTimeRdd, 10, 29)
    val deep_30_59 = timeLength_x(useTimeRdd, 30, 59)
    val deep_1m_3m = timeLength_x(useTimeRdd, 60, 60 * 3 - 1)
    val deep_3m_10m = timeLength_x(useTimeRdd, 60 * 3, 60 * 10 - 1)
    val deep_10m_30m = timeLength_x(useTimeRdd, 60 * 10, 60 * 30 - 1)
    val deep_30m = timeLength_x(useTimeRdd, 60 * 30, Int.MaxValue)

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:useTimeLength-perday"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("0_3"), Bytes.toBytes(deep_0_3))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("4_9"), Bytes.toBytes(deep_4_9))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("10_29"), Bytes.toBytes(deep_10_29))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("30_59"), Bytes.toBytes(deep_30_59))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("1m_3m"), Bytes.toBytes(deep_1m_3m))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("3m_10m"), Bytes.toBytes(deep_3m_10m))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("10m_30m"), Bytes.toBytes(deep_10m_30m))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("30m"), Bytes.toBytes(deep_30m))
      table.put(put)
    } finally {
      table.close()
    }

  }

  def timeLength_x(rdd: RDD[(Int, Int)], start: Int, end: Int) = {
    rdd.filter(r => r._1 >= start && r._1 <= end).map(e => e._2).reduce(_ + _)
  }


}
