package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.BehaviorEvent
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

class UserBehaviorEventExecutor(startTime: String, endTime: String, tableName: String, statTime: String, savaTable: String) extends CoreExecutor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime + StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[XCXTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

   /**
      * 基于用户统计事件点击次数
      */
    logger.info(" 开始计算基于用户行为事件统计......")
    hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if (rowKeys.length == 7) {
        val appid = rowKeys(2)
        val page_type = rowKeys(3)
        val page_id = rowKeys(4)
        val event_id = rowKeys(5)
        val user_id = rowKeys(6)
        val key = appid + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + event_id + StatisticsConstant.KEY_SEPARATOR_C + user_id
        List((key, 1L))
      } else {
        List()
      }
    }).reduceByKey((before, after) => {
      before + after
    }).foreachPartition(iterator => {
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext) {
        val tuple2 = iterator.next();
        val key = tuple2._1
        val value = tuple2._2
        val updateRequest = BehaviorEvent.generateUserBehaviorEventJson(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })

    logger.info("基于用户的事件统计计算完毕.")


    /**
      * 基于页面统计事件
      */
    logger.info("开始计算基于页面的事件统计......")
    hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if (rowKeys.length == 7) {
        val appid = rowKeys(2)
        val page_type = rowKeys(3)
        val page_id = rowKeys(4)
        val event_id = rowKeys(5)
        val user_id = rowKeys(6)
        val key = appid + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id + StatisticsConstant.KEY_SEPARATOR_C + event_id
        List((key, 1L))
      } else {
        List()
      }
    }).reduceByKey((before, after) => {
      before + after
    }).foreachPartition(iterator => {
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext) {
        val tuple2 = iterator.next();
        val key = tuple2._1
        val value = tuple2._2
        val updateRequest = BehaviorEvent.generatePageEventJson(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })
    logger.info("基于页面的事件统计计算完毕.")

  }

}
