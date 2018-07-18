package com.cloudyoung.bigdata.miniprogram.statistics.test

import java.util.Date

import com.cloudyoung.bigdata.common.Constant
import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.common.util.TimeUtils
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import com.cloudyoung.bigdata.miniprogram.statistics.executor.CountExecutor
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlHbase{

  def main(args: Array[String]): Unit = {

    val date = new Date()
    println(TimeUtils.getHour(date, -1, StatisticsConstant.HOUR_PATTERN))
    println(TimeUtils.getDay(date, -1, StatisticsConstant.HOUR_PATTERN))
    val cluster = "master:2181"
    val timeout = 6000
    Constant.init(cluster, timeout)
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE,"young_dev:xcx_event_tracking_record")
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, "20180425"+StatisticsConstant.SPLIT_TIME_C + "20180426" )
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    var hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,classOf[XCXTableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    val tableCase = hbaseRDD.map(r => {
      val row = Bytes.toString(r._2.getRow)
      val rows = StringUtils.split(row, StatisticsConstant.KEY_SEPARATOR_C)
      (
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("style_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("serial_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("province_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("city_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("deal_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("open_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("user_id"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("page_source_url"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("page_url"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("start_time"))),
        Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("end_id"))),
        rows(1),
        rows(2),
        rows(3)
      )
    }).toDF("style_id", "serial_id", "province_id", "city_id", "deal_id", "open_id", "user_id","page_source_url", "page_url", "start_time", "end_id", "time", "trace_id", "event_id" )
    tableCase.registerTempTable("record")
    val countExecutor = new CountExecutor()
    countExecutor.executor(sqlContext, tableCase)
  }

}
