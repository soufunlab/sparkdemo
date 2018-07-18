package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.UserDetailGenerateJson
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput.ElasticSearchClient
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

class UserEventExecutor(startTime: String, endTime: String, tableName: String, statTime: String, savaTable: String) extends CoreExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass);
  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime + StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[XCXTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    logger.info("开始计算用户事件.....")
    hbaseRDD.flatMap(tuple2 =>{
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if(rowKeys.length == 5){
        val appid = rowKeys(2)
        val event_id = rowKeys(3)
        val user_id = rowKeys(4)
        val key = appid + StatisticsConstant.KEY_SEPARATOR_C + event_id + StatisticsConstant.KEY_SEPARATOR_C + user_id
        List(key)
      } else {
        List()
      }
    }).distinct().foreachPartition(iterator => {
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext){
        val key = iterator.next()
        val updateRequest = UserDetailGenerateJson.generateUserEvenJson(statTime, key)
        elasticSearchClient.add(updateRequest)
      }
      elasticSearchClient.close()
    })
    logger.info("用户事件计算完毕.....")
  }
}
