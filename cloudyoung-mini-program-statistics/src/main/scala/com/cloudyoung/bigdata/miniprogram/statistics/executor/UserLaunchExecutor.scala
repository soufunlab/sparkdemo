package com.cloudyoung.bigdata.miniprogram.statistics.executor
import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.{StatisticsConstant, TargetConstant}
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.UserLaunch
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput.ElasticSearchClient
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output.SaveDataHBaseImpl
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

/**
  * 新增用户统计
  * @param startTime 计算开始时间
  * @param endTime 计算结束时间
  * @param tableName 计算获取数据表名
  * @param statTime 计算保存数据时间
  * @param saveTable 计算结果保存表名
  */
class UserLaunchExecutor(startTime:String, endTime:String, tableName:String, statTime:String, saveTable:String) extends CoreExecutor {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def executor(sparkContext: SparkContext): Unit = {

    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime+StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf,classOf[XCXTableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])

    logger.info("开始计算用户启动。。。")
    hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
        if (rowKeys.length == 4) {
          val appid = rowKeys(2)
          val user_id = rowKeys(3)
          val key = appid + StatisticsConstant.KEY_SEPARATOR_C + user_id
          List((key,1L))
        } else {
          List()
        }
    }).reduceByKey((pre, after) => {pre + after}).foreachPartition(iter => {
      val elasticSearchClient = new ElasticSearchClient()
      while (iter.hasNext) {
        val tuple = iter.next()
        val key = tuple._1
        val value = tuple._2
        val updateRequest = UserLaunch.generateUserLaunchNum(statTime,key,value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }

      }
      elasticSearchClient.close()
    })


  }
}
