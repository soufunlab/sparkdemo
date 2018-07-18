package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.{StatisticsConstant, TableConstant}
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output.SaveDataHBaseImpl
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

class DistributionExecutor(val tableName: String, val time: String) extends CoreExecutor {
  var table: String = tableName
  var startTime: String = time

  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, table)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, "20180425" + StatisticsConstant.SPLIT_TIME_C + "20180426")
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[XCXTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    hbaseRDD.flatMap(results => {
      try {
        val result = results._2
        val row = Bytes.toString(result.getRow)
        val rows = StringUtils.split(row, StatisticsConstant.KEY_SEPARATOR_C)
        (rows(1) + rows(2))
      } catch {
        case e: Exception => ("" + 0)
      }
    }

    )


    hbaseRDD.map(results => {
      try {
        val result = results._2
        val row = Bytes.toString(result.getRow)
        val rows = StringUtils.split(row, StatisticsConstant.KEY_SEPARATOR_C)
        if (rows.length != 5) {
          ("" + 0, 0)
        } else {
          val key = rows(1) + StatisticsConstant.KEY_SEPARATOR_C + rows(2) + StatisticsConstant.KEY_SEPARATOR_C + rows(4)
          val value = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("scene_value")))
          (key, value.toInt)
        }
      } catch {
        case e: Exception => ("" + 0, 0)
      }
    }).reduceByKey((pre, after) => (pre + after)).foreachPartition(iter => {
      val savaData = new SaveDataHBaseImpl(TableConstant.result_module_day_stat, StatisticsConstant.family)
      while (iter.hasNext) {
        val moudle_id = 0
        val tuple = iter.next()
        val key = tuple._1
        val value = tuple._2
        val rows = StringUtils.split(key, StatisticsConstant.KEY_SEPARATOR_C)
        if (rows.length < 3) {
          val time = rows(0)
          val appid = rows(1)
          val column = rows(2)
          val rowkey = time + StatisticsConstant.KEY_SEPARATOR_C + appid + StatisticsConstant.KEY_SEPARATOR_C + moudle_id
          savaData.saveRowData(rowkey, column, value.toString)
        }
      }
      savaData.closeTable()
    })
  }

}
