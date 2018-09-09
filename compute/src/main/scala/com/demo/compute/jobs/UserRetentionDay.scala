package com.demo.compute.jobs

import java.util.{Calendar, Date}

import com.demo.compute.coms.{LogObj, Utils}
import com.demo.compute.jobs.UserRetentionDay.date
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object UserRetentionDay {
  var date: Date = null
  var jobName = ""

  var source_table = ""
  var target_table = ""

  var register_table = "compute:newuser_day"
  var startups_table = "compute:userstartup_day"

  def main(args: Array[String]): Unit = {
    this.switch(args)
    val conf = new SparkConf().setAppName(this.jobName)
      .setMaster("local")
    val sc = new SparkContext(conf)


    val nowUsersRdd = nowRdd(sc).keys.map(k => (Bytes.toString(k.get())).split("_")(1)).map(k => (k, 1))

    val days = daysList()
    for (day <- days) {
      val history = historyRdd(sc, day._1).keys.map(k => (Bytes.toString(k.get())).split("_")(1)).map(k => (k, 1))
      val jrdd = nowUsersRdd.join(history)
      val retentionCount = jrdd.count()
      val historyCount = history.count()
      val rlv = historyCount match {
        case o => -1
        case _ => retentionCount / historyCount
      }
      val table = Utils.hbaseConn.getTable(TableName.valueOf(target_table))
      try {
        val put = new Put(Bytes.toBytes(day._2))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ht"), Bytes.toBytes(historyCount))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rt"), Bytes.toBytes(retentionCount))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rlv"), Bytes.toBytes(rlv))
        table.put(put)
      } finally {
        table.close()
      }
    }
  }

  def historyRdd(sc: SparkContext, date: String) = {
    val scan = {
      var scani = new Scan()

      scani.setFilter(new PrefixFilter(Bytes.toBytes(date)))
      val proto = ProtobufUtil.toScan(scani)
      Base64.encodeBytes(proto.toByteArray)
    }

    val hconf = Utils.hbaseConf
    hconf.set(TableInputFormat.INPUT_TABLE, this.source_table)
    hconf.set(TableInputFormat.SCAN, scan)
    sc.newAPIHadoopRDD(hconf,
      classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
  }

  def nowRdd(sc: SparkContext) = {
    historyRdd(sc, Utils.hbaseDay(date))
  }

  def switch(args: Array[String]) = {
    var t = args match {
      case Array(x, y) =>
        this.date = Utils.executeTime(y)
        x
      case (x) =>
        this.date = Utils.executeTime("")
        x
    }
    if (t == "newuser") {
      this.jobName = "newuser_retention_day"
      this.source_table = this.register_table
      this.target_table = "newuser_retention_day"
    } else {
      this.jobName = "activeuser_retention_day"
      this.source_table = this.startups_table
      this.target_table = "activeuser_retention_day"
    }
  }

  def daysList() = {
    var cal = Calendar.getInstance()
    cal.setTime(this.date)
    var list = for (i <- 1 to 7)
      yield {
        cal.add(Calendar.DATE, -1)
        var key = Utils.hbaseDay(cal.getTime)
        (key, key + "_" + i)
      }

    var _15 = {
      cal.add(Calendar.DATE, -8)
      var key = Utils.hbaseDay(cal.getTime)
      (key, key + "_" + 15)
    }
    list = list :+ _15

    var _30 = {
      cal.add(Calendar.DATE, -15)
      var key = Utils.hbaseDay(cal.getTime)
      (key, key + "_" + 30)
    }
    list = list :+ _30

    list
  }

}
