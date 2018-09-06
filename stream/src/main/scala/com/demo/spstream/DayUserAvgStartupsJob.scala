package com.demo.spstream

import com.demo.spstream.utils.Utils
import com.demo.spstream.utils.Utils.Entity
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-5 11:18 by 李浩（lihao@cloud-young.com）创建
  */
object DayUserAvgStartupsJob {
  def execute(sc: SparkContext, hour: String): Unit = {
    val allStartupCount = {
      val scan = {
        var scan = new Scan()
        scan.setFilter(new PrefixFilter(Bytes.toBytes(hour.split(" ")(0))))
        val proto = ProtobufUtil.toScan(scan)
        Base64.encodeBytes(proto.toByteArray)
      }

      val conf = Utils.hbaseConf
      conf.set(TableInputFormat.INPUT_TABLE, "compute:startups_hour")
      conf.set(TableInputFormat.SCAN, scan)
      val hbaseRdd = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      if (!hbaseRdd.isEmpty()) {
        val startups = hbaseRdd.values.map {
          r =>
            val count = Bytes.toString(r.getRow)
            count.toInt
        }.reduce(_ + _)
        startups
      }
      0
    }

    val userCount = {
      val scan={
        var scan = new Scan()
        scan.setFilter(new PrefixFilter(Bytes.toBytes(hour.split(" ")(0))))
        val proto = ProtobufUtil.toScan(scan)
        Base64.encodeBytes(proto.toByteArray)
      }
      val conf = Utils.hbaseConf
      conf.set(TableInputFormat.INPUT_TABLE, "compute:newuser_day")
      conf.set(TableInputFormat.SCAN, scan)

      val hbaseRdd = sc.newAPIHadoopRDD(conf,
        classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      hbaseRdd.count()
    }
    println("userCount" + userCount)
    val avgUserStartups = userCount match {
      case 0 => -1
      case _ => allStartupCount / userCount
    }
    val mean = Utils.hbaseConn.getTable(TableName.valueOf("compute:useravg_startups_day"))
    val put = new Put(Bytes.toBytes(hour.split(" ")(0)))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("tl"), Bytes.toBytes(avgUserStartups.toString))
    mean.put(put)
  }
}
