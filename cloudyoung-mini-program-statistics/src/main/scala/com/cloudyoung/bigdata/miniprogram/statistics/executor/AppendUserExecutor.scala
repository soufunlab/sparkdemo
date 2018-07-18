package com.cloudyoung.bigdata.miniprogram.statistics.executor
import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.{StatisticsConstant, TargetConstant}
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output.SaveDataHBaseImpl
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

/**
  * 新增用户统计
  * @param startTime 计算开始时间
  * @param endTime 计算结束时间
  * @param tableName 计算获取数据表名
  * @param statTime 计算保存数据时间
  * @param savaTable 计算结果保存表名
  */
class AppendUserExecutor(startTime:String, endTime:String, tableName:String, statTime:String, savaTable:String) extends CoreExecutor {

  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime+StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf,classOf[XCXTableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
    hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if(rowKeys.length != 4){
        List()
      } else {
        var list = List((rowKeys(2), 1L),
          ("" + -1,1L)
        )
        list
      }
    }).reduceByKey((pre, after) => {
      pre + after
    }).foreachPartition(iterator => {
      val savaData = new SaveDataHBaseImpl(savaTable, StatisticsConstant.family)
      while(iterator.hasNext){
        var tuple2 = iterator.next();
        var key = tuple2._1;
        var value = tuple2._2
        var rowKey = statTime  + StatisticsConstant.KEY_SEPARATOR_C + key
        savaData.saveRowData(rowKey, TargetConstant.append_num, value.toString)
      }
      savaData.closeTable();
    })
  }
}
