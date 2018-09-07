package com.demo.compute.jobs

import java.util.Date

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
object UvWeek {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)

    val days = Utils.weekdays(this.date)
    val days1 = Utils.monthdays(this.date)
    //    val conf = new SparkConf().setAppName("uv-week")
    //      .setMaster("local")
    //    val sc = new SparkContext(conf)
    //
    //    val scan = {
    //      var scani = new Scan()
    //      scani.setFilter(new PrefixFilter(Bytes.toBytes(Utils.hbaseDay(this.date))))
    //      val proto = ProtobufUtil.toScan(scani)
    //      Base64.encodeBytes(proto.toByteArray)
    //    }
    //    val hconf = Utils.hbaseConf
    //    hconf.set(TableInputFormat.INPUT_TABLE, "compute:newuser_day")
    //    hconf.set(TableInputFormat.SCAN_COLUMNS, "cf1:ct")
    //    hconf.set(TableInputFormat.SCAN, scan)
    //
    //    val hbaseRdd = sc.newAPIHadoopRDD(hconf,
    //      classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    //      classOf[org.apache.hadoop.hbase.client.Result])
    //
    //    val dayUv = hbaseRdd.count()
    //
    //    val table = Utils.hbaseConn.getTable(TableName.valueOf("compute:uv_day"))
    //    try {
    //      val put = new Put(Bytes.toBytes(Utils.hbaseDay(this.date)))
    //      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("ct"), Bytes.toBytes(dayUv))
    //      table.put(put)
    //    } finally {
    //      table.close()
    //    }

  }


}
