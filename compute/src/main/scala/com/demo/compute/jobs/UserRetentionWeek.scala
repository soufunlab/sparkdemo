package com.demo.compute.jobs

import java.util.{Calendar, Date}

import com.demo.compute.coms.Utils
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
object UserRetentionWeek {
  var date: Date = null
  var jobName = ""

  var source_table = ""
  var target_table = ""

  var register_table = "compute:newuser_day"
  var startups_table = "compute:userstartup_day"

  def main(args: Array[String]): Unit = {
    this.switch(args)
    val conf = new SparkConf().setAppName(this.jobName)
//      .setMaster("local")
    val sc = new SparkContext(conf)
    val weeks = weekList()

    val nowUsersRdd = nowRdd(sc, this.thisWeekStartEnd()).keys.map(k => (Bytes.toString(k.get())).split("_")(1)).map(k => (k, 1))

    for (week <- weeks) {
      val history = historyRdd(sc, week._1).keys.map(k => (Bytes.toString(k.get())).split("_")(1)).map(k => (k, 1))
      val jrdd = nowUsersRdd.join(history)
      val retentionCount = jrdd.count()
      val historyCount = history.count()

      val table = Utils.hbaseConn.getTable(TableName.valueOf(target_table))
      try {
        val put = new Put(Bytes.toBytes(week._2))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ht"), Bytes.toBytes(historyCount.toString))
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("rt"), Bytes.toBytes(retentionCount.toString))
        table.put(put)
      } finally {
        table.close()
      }
    }
  }

  def historyRdd(sc: SparkContext, startEnd: (String, String)) = {

    val scan = {
      val cal = Calendar.getInstance()
      cal.setTime(Utils.hbaseDay2Date(startEnd._2))
      cal.add(Calendar.DATE, 1)

      var scani = new Scan()
      scani.setStartRow(Bytes.toBytes(startEnd._1))
      scani.setStopRow(Bytes.toBytes(Utils.hbaseDay(cal.getTime)))
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

  def nowRdd(sc: SparkContext, startEnd: (String, String)) = {
    historyRdd(sc, startEnd)
  }

  def switch(args: Array[String]) = {
    var t = {
      if (args.length == 2) {
        this.date = Utils.executeTime(args(1))
        args(0)
      } else {
        this.date = Utils.executeTime("")
        args(0)
      }
    }
    if (t == "newuser") {
      this.jobName = "newuser_retention_week"
      this.source_table = this.register_table
      this.target_table = "compute:newuser_retention_week"
    } else {
      this.jobName = "activeuser_retention_week"
      this.source_table = this.startups_table
      this.target_table = "compute:activeuser_retention_week"
    }
  }

  def thisWeekStartEnd() = {
    val cal = Calendar.getInstance()
    cal.setTime(this.date)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    val start = Utils.hbaseDay(cal.getTime)
    val end = Utils.hbaseDay(this.date)
    (start, end)
  }

  def weekList() = {
    var cal = Calendar.getInstance()
    cal.setTime(this.date)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)
    val result = for (i <- 1 to 9)
      yield {
        cal.add(Calendar.DATE, -7)
        var cli = Calendar.getInstance()
        cli.setTime(cal.getTime)
        cli.add(Calendar.DATE, 6)
        val start = Utils.hbaseDay(cal.getTime)
        val end = Utils.hbaseDay(cli.getTime)
        ((start, end), start + "_" + i)
      }
    result
  }

}
