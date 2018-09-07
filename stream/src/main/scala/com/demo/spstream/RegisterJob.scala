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
object RegisterJob {
  def execute(rdd: RDD[(String, String)], hour: String): Unit = {
    val prdd = rdd.map(i => i._2).map(i => i.split("\t")).map { i =>
      i match {
        case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
        => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      }
    }.filter(e => e.event == Utils.event_register)
      .map(e => (e.openid, e)).reduceByKey((a, b) => a).map(e => e._2).persist();

    /**
      * 新增用户数进小时表
      */
    putNewUserHour(prdd, hour)

    /**
      * 新增用户进当日新增用户表
      */
    putNewUserDay(prdd, hour)

    /**
      * 新增用户进总用户表
      */
    putNewUserAll(prdd)
  }

  /**
    * 新增用户进日表
    *
    * @param prdd
    * @param hour
    */
  def putNewUserDay(prdd: RDD[Entity], hour: String) = {
    val key_prefix = hour.split(" ")(0);
    prdd.map(e => {
      val put = new Put(Bytes.toBytes(key_prefix + "_" + e.openid))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(e.province.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes(e.city.toString))
    }).foreachPartition(it => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:newuser_day"))
      try {
        table.put(it.toList)
      } finally {
        table.close()
      }
    })
  }

  def putNewUserHour(prdd: RDD[Entity], hour: String) = {
//    var hours = hour.replace(" ", "")
    val count = prdd.count()
    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:newuser_hour"))
    val put = new Put(Bytes.toBytes(hour))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(count.toString))
    try {
      table.put(put)
    } finally {
      table.close()
    }
  }

  def putNewUserAll(prdd: RDD[Entity]) = {
    prdd.map(e => {
      val put = new Put(Bytes.toBytes(e.openid))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("p"), Bytes.toBytes(e.province.toString))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c"), Bytes.toBytes(e.city.toString))
    }).foreachPartition(it => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:all_user"))
      try {
        table.put(it.toList)
      } finally {
        table.close()
      }

    })
  }
}
