package com.demo.compute.jobs

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
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
      .setMaster("local")
    val sc = new SparkContext(conf)

    Utils.setHadoopConf(sc.hadoopConfiguration)

    val path = s"hdfs://nameservice1/user/root/test/${Utils.dfs_date(date)}/${Utils.dfs_date(date)}.txt"
    val hadoopRdd = sc.textFile(path).map(i => i.split("\t")).map(i => i match {
      case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      => LogObj(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    })

    val left_rdd = hadoopRdd.filter(e => e.event == Utils.event_load).map(e => (e.traceid, e))
    val right_rdd = hadoopRdd.filter(e => e.event == Utils.event_exit).map(e => (e.traceid, e))
    val join_rdd = left_rdd.join(right_rdd)
    join_rdd.persist()

    val timeLength = join_rdd.map(e => e._2).map(e => Utils.longTime(e._2.time) - Utils.longTime(e._1.time)).reduce(_ + _)
    val count = join_rdd.map(e => e._2._1).map(e => e.openid).distinct().count()

    val meanTime = count match {
      case 0 => -1
      case _ => timeLength / count
    }

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:avguser_time_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(meanTime))
      table.put(put)
    } finally {
      table.close()
    }

  }


}
