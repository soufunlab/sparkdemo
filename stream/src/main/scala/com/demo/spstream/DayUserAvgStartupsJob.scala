package com.demo.spstream

import com.demo.spstream.utils.Utils
import com.demo.spstream.utils.Utils.Entity
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Put, Scan}
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
object StartupsJob {
  def execute(sc: SparkContext, hour: String): Unit = {
    val allStartupCount = {
      val conf = Utils.hbaseConf
      conf.set(TableInputFormat.INPUT_TABLE, "compute:startups_hour")
      conf.set(TableInputFormat.SCAN_ROW_START, hour.split(" ")(0))
      conf.set(TableInputFormat.SCAN_ROW_STOP, hour.split(" ")(0))
      conf.set(TableInputFormat.SCAN_COLUMNS, "cf1:ct")
      val hbaseRdd = sc.newAPIHadoopRDD(Utils.hbaseConf,
        classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      val startups = hbaseRdd.values.map {
        r =>
          val count = Bytes.toString(r.getRow)
          try {
            count.toInt
          } catch {
            case Exception => 0
          }
      }.reduce(_ + _)
      startups
    }

    val userCount = {
      val conf = Utils.hbaseConf
      conf.set(TableInputFormat.INPUT_TABLE, "compute:newuser_day")
      conf.set(TableInputFormat.SCAN_ROW_START, hour.split(" ")(0))
      conf.set(TableInputFormat.SCAN_ROW_STOP, hour.split(" ")(0))
      conf.set(TableInputFormat.SCAN_COLUMNS, "cf1:p")
      val hbaseRdd = sc.newAPIHadoopRDD(Utils.hbaseConf,
        classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])

      hbaseRdd.count()
    }

    val avgUserStartups = allStartupCount / userCount
    val mean = Utils.hbaseConn.getTable(TableName.valueOf("compute:useravg4_startups_day"))
    val put = new Put(Bytes.toBytes(hour.split(" ")(0)))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("tl"), Bytes.toBytes(avgUserStartups.toString))
    mean.put(put)
  }
}
