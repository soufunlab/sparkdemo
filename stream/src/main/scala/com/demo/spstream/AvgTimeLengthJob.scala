package com.demo.spstream

import java.text.SimpleDateFormat

import com.demo.spstream.utils.Utils
import com.demo.spstream.utils.Utils.Entity
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-5 11:18 by 李浩（lihao@cloud-young.com）创建
  */
object AvgTimeLengthJob {
  def execute(rdd: RDD[(String, String)], hour: String): Unit = {
    val inputRdd = rdd.map(i => i._2).map(i => i.split("\t")).map { i =>
      i match {
        case Array(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
        => Entity(time, openid, traceid, sourceurl, pageurl, staytime, province, city, event, device, os)
      }
    }
    val rddStart = inputRdd.filter(e => e.event == Utils.event_load).map(e => (e.openid + "_" + e.traceid, e.time))
    val rddExit = inputRdd.filter(e => e.event == Utils.event_exit).map(e => (e.openid + "_" + e.traceid, e.time))
    val jrdd = rddStart.join(rddExit).persist()
    putAvgTimeHour(jrdd, hour)
  }

  /**
    * 城市维度统计启动次数
    *
    * @param hour
    */
  def putAvgTimeHour(jrdd: RDD[(String, (String, String))], hour: String) = {
    val allTraceCount = jrdd.count
    val key_prefix = hour.split(" ")(0);

    val timeLength = jrdd.values.mapPartitions(it => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      it.map(e => {
        val start = sdf.parse(e._1)
        val end = sdf.parse(e._2)
        end.getTime - start.getTime
      })
    }).reduce(_ + _)

    val meanTimeLength = timeLength / allTraceCount

    val userCount = jrdd.keys.map(k => k.split("_")(0)).distinct().count()
    val avgUserTimeLength = timeLength / userCount

    val mean = Utils.hbaseConn.getTable(TableName.valueOf("compute:mean_time_hour"))
    val put = new Put(Bytes.toBytes(hour))
    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("tl"), Bytes.toBytes(meanTimeLength.toString))
    mean.put(put)

    val avguser = Utils.hbaseConn.getTable(TableName.valueOf("compute:avguser_time_hour"))
    val avgput = new Put(Bytes.toBytes(hour))
    avgput.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("tl"), Bytes.toBytes(avgUserTimeLength.toString))
    avguser.put(avgput)
  }

}
