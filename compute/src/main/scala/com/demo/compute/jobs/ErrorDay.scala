package com.demo.compute.jobs

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.commons.lang3.StringUtils
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
object ErrorDay {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("error-day")
      .setMaster("local")
    val sc = new SparkContext(conf)

    Utils.setHadoopConf(sc.hadoopConfiguration)

    val path = s"hdfs://nameservice1/user/root/test/${Utils.dfs_date(date)}/${Utils.dfs_date(date)}.txt"
    val hadoopRdd = sc.textFile(path).map(i => i.split("\t")).map(i => i match {
      case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      => LogObj(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
    })

    val count = hadoopRdd.filter(e => e.event == Utils.event_error)
      .map(e => e.openid).distinct().count()

    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:error_day"))
    try {
      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(count))
      table.put(put)
    } finally {
      table.close()
    }

  }


}
