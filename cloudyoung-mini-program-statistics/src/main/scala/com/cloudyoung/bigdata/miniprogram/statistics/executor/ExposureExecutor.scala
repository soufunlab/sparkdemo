package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.{BrowsePage, Exposure}
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput.ElasticSearchClient
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

class ExposureExecutor(startTime: String, endTime: String, tableName: String, statTime: String, savaTable: String) extends CoreExecutor {
  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime + StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[XCXTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if (rowKeys.length == 7) {
        val appid = rowKeys(2)
        val exposure_type = rowKeys(3)
        val page_type = rowKeys(4)
        val page_id = rowKeys(5)
        val user_id = rowKeys(6)
        val key = appid + StatisticsConstant.KEY_SEPARATOR_C + exposure_type + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
        val all_exposure_Key = appid + StatisticsConstant.KEY_SEPARATOR_C + 0 + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
        List((key, 1L), (all_exposure_Key, 1L))
      } else {
        List()
      }
    }).reduceByKey((before, after) => {
      before + after
    }).foreachPartition(iterator =>{
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext) {
        val tuple2 = iterator.next()
        val key = tuple2._1
        val value = tuple2._2
        val updateRequest = Exposure.generateExposurePage(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })


  }
}
