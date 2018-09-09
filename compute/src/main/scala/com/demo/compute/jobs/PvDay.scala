package com.demo.compute.jobs

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object PvDay {
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

    pageViewCount(hadoopRdd)
    meanTimeLong(hadoopRdd)
    incount(hadoopRdd)
    outcount(hadoopRdd)
  }

  def outcount(hadoopRdd: RDD[LogObj]) = {
    hadoopRdd.filter(e => e.event == Utils.event_exit).map(e => (e.pageurl, 1))
      .reduceByKey(_ + _).foreachPartition(itr => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:pv_day"))
      try {
        itr.foreach(i => {
          val put = new Put(Bytes.toBytes(i._1))
          put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("oct"), Bytes.toBytes(i._2))
          table.put(put)
        })
      } finally {
        table.close()
      }
    })
  }

  def incount(hadoopRdd: RDD[LogObj]) = {
    hadoopRdd.filter(e => e.event == Utils.event_load).map(e => (e.pageurl, 1))
      .reduceByKey(_ + _).foreachPartition(itr => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:pv_day"))
      try {
        itr.foreach(i => {
          val put = new Put(Bytes.toBytes(i._1))
          put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ict"), Bytes.toBytes(i._2))
          table.put(put)
        })
      } finally {
        table.close()
      }
    })
  }

  def meanTimeLong(hadoopRdd: RDD[LogObj]) = {
    hadoopRdd.map(e => (e.pageurl, (e.staytime.toInt, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(i => i._1 / i._2)
      .foreachPartition(itr => {
        val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:pv_day"))
        try {
          itr.foreach(i => {
            val put = new Put(Bytes.toBytes(i._1))
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("mtl"), Bytes.toBytes(i._2))
            table.put(put)
          })
        } finally {
          table.close()
        }
      })

  }

  def pageViewCount(hadoopRdd: RDD[LogObj]) = {
    val countRdd = hadoopRdd.map(e => (e.pageurl, 1)).reduceByKey(_ + _)
    countRdd.foreachPartition(r => {
      val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:pv_day"))
      try {
        r.foreach(i => {
          val put = new Put(Bytes.toBytes(i._1))
          put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(i._2))
          table.put(put)
        })
      } finally {
        table.close()
      }
    })
  }
}
