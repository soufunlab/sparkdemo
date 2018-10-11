package com.demo.compute.jobs

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object AvgUserTimeDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("avguser-time-day")
    //      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new HiveContext(sc)

    import sqlCtx.implicits._
    import sqlCtx.sql

    //    val df = sql(
    //      s"""
    //         |select sum(b.time -a.time ) as tl,count(distinct(a.openid)) from
    //         |(select openid,unix_timestamp(time,'yyy-MM-dd HH:mm:ss') time from source_data where date='20181010' and event='load') as a
    //         |join
    //         |(select openid,unix_timestamp(time,'yyy-MM-dd HH:mm:ss') time from source_data where date='20181010' and event='exit') as b
    //         |on a.openid=b.openid
    //      """.stripMargin)

    val df = sql(
      s"""
         |select sum(a.exit)-sum(a.load) as tl,count(distinct(a.openid)) as count
         |from
         |(select openid
         |,(case  when event='load' then -unix_timestamp(time,'yyy-MM-dd HH:mm:ss') else 0 end)  load
         |,(case when event='exit' then unix_timestamp(time,'yyy-MM-dd HH:mm:ss') else 0 end)  exit
         |from source_data where date='20181010' and event='load' or event='exit') as a
      """.stripMargin).as[(Long, Long)]

    val row = df.collect()(0)
    val (tl, count) = (row._1, row._2)

    val meanTime = count match {
      case 0 => -1
      case _ => tl / count
    }

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:avguser_time_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(meanTime.toString))
      table.put(put)
    } finally {
      table.close()
    }

  }


}
