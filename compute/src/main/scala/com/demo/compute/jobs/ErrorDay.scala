package com.demo.compute.jobs

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object ErrorDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("error-day")
    //      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    import sqlContext.implicits._
    import sqlContext.sql

    val df = sql(s"select count(distinct(openid)) as count from source_data where date=${date} and event=${Utils.event_error}")
    val count = df.collect()(0).getAs[Int]("count")
    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:error_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(count.toString))
      table.put(put)
    } finally {
      table.close()
    }

  }


}
