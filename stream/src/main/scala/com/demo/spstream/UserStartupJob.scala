package com.demo.spstream

import com.demo.spstream.utils.Utils
import com.demo.spstream.utils.Utils.Entity
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-5 11:18 by 李浩（lihao@cloud-young.com）创建
  */
object UserStartupJob {
  def execute(rdd: RDD[(String, String)], hour: String): Unit = {
    val prdd = rdd.map(i => i._2).map(i => i.split("\t")).map { i =>
      i match {
        case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
        => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      }
    }.filter(e => e.event == Utils.event_load).map(e => (e.openid, (e.province, e.city))).reduceByKey((a, b) => a).persist();

    putStartupHour(prdd, hour)
    putStartupDay(prdd, hour)

  }

  /**
    * 启动用户进日表
    *
    * @param prdd
    * @param hour
    */
  def putStartupDay(prdd: RDD[(String, (String, String))], hour: String) = {
    val key_prefix = hour.split(" ")(0);
    prdd.map(e => {
      val put = new Put(Bytes.toBytes(key_prefix + "_" + e._1))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(e._2._1.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes(e._2._1.toString))
    }).foreachPartition(it => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:userstartup_day"))
      try {
        table.put(it.toList)
      } finally {
        table.close()
      }
    })
  }

  /**
    * 启动用户进小时表
    *
    * @param prdd
    * @param hour
    */
  def putStartupHour(prdd: RDD[(String, (String, String))], hour: String) = {
//    var hours = hour.replace(" ", "")
    val count = prdd.map(e => e._1).distinct().count()
    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:userstartup_hour"))
    try {
      val put = new Put(Bytes.toBytes(hour))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(count.toString))
      table.put(put)
    } finally {
      table.close()
    }
  }

}
