package com.demo.compute.jobs

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.commons.lang3.StringUtils
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
object MeanTimeDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("mean-time-day")
    //      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new HiveContext(sc)

    import sqlCtx.implicits._
    import sqlCtx.sql

    val df = sql(
      s"""
         |select sum(b.time -a.time) as tl,count(distinct(a.traceid)) from
         |(select traceid,unix_timestamp(time,'yyy-MM-dd HH:mm:ss') time from source_data where date='20181010' and event='load') as a
         |join
         |(select traceid,unix_timestamp(time,'yyy-MM-dd HH:mm:ss') time from source_data where date='20181010' and event='exit') as b
         |on a.traceid=b.traceid
      """.stripMargin)

    //    Utils.setHadoopConf(sc.hadoopConfiguration)
    //
    //    val path = s"hdfs://nameservice1/user/root/test/${Utils.dfs_date(date)}/${Utils.dfs_date(date)}.txt"
    //    val hadoopRdd = sc.textFile(path).map(i => i.split("\t")).map(i => i match {
    //      case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    //      => LogObj(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    //    })
    //
    //    val left_rdd = hadoopRdd.filter(e => e.event == Utils.event_load).map(e => (e.traceid, e))
    //    val right_rdd = hadoopRdd.filter(e => e.event == Utils.event_exit).map(e => (e.traceid, e))
    //    val join_rdd = left_rdd.join(right_rdd)
    //    join_rdd.persist()
    //
    //    val timeLength = join_rdd.map(e => e._2).map(e => Utils.longTime(e._2.time) - Utils.longTime(e._1.time)).reduce(_ + _)
    //    val count = join_rdd.count()
    val row = df.collect()(0)
    val (tl, count) = (row.getAs[Long](0), row.getAs[Int](1))

    val meanTime = count match {
      case 0 => -1
      case _ => tl / count
    }

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:mean_time_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(meanTime.toString))
      table.put(put)
    } finally {
      table.close()
    }

  }


}
