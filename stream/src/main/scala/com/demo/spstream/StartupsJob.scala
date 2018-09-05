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
object StartupsJob {
  def execute(rdd: RDD[(String, String)], hour: String): Unit = {
    val prdd = rdd.map(i => i._2).map(i => i.split("\t")).map { i =>
      i match {
        case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
        => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      }
    }.filter(e => e.event == Utils.event_load).persist();

    putStartupsHour(prdd, hour)
    putStartupsOsDay(prdd, hour)
    putStartupsCityDay(prdd, hour)
  }

  /**
    * 城市维度统计启动次数
    *
    * @param prdd
    * @param hour
    */
  def putStartupsCityDay(prdd: RDD[Entity], hour: String) = {
    val key_prefix = hour.split(" ")(0);
    prdd.map(e => (e.city, 1)).reduceByKey(_ + _).foreach(e => {
      val put = new Put(Bytes.toBytes(key_prefix + "_" + e._1))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(e._2.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes(e._2.toString))
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:startups_city_day"))
      table.put(put)
    })
  }

  /**
    * os维度统计启动次数
    *
    * @param prdd
    * @param hour
    */
  def putStartupsOsDay(prdd: RDD[Entity], hour: String) = {
    val key_prefix = hour.split(" ")(0);
    prdd.map(e => (e.os, 1)).reduceByKey(_ + _).foreach(e => {
      val put = new Put(Bytes.toBytes(key_prefix + "_" + e._1))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(e._2.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes(e._2.toString))
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:startups_os_day"))
      table.put(put)
    })
  }


  /**
    * 启动用户进小时表
    *
    * @param prdd
    * @param hour
    */
  def putStartupsHour(prdd: RDD[Entity], hour: String) = {
    val count = prdd.count()
    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:startups_hour"))
    val put = new Put(Bytes.toBytes(hour))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(count.toString))
    table.put(put)
  }

}
