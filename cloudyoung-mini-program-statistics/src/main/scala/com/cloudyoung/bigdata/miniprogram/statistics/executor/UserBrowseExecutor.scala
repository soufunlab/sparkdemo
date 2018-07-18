package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.{BehaviorEvent, BrowsePage}
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput.ElasticSearchClient
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

class UserBrowseExecutor(startTime: String, endTime: String, tableName: String, statTime: String, savaTable: String) extends CoreExecutor {
  override def executor(sparkContext: SparkContext): Unit = {
    val conf = HBaseContext.getHbaseConf()
    val hBaseConf = HBaseConfiguration.create(conf)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hBaseConf.set(StatisticsConstant.CUSTOM_TIME, startTime + StatisticsConstant.SPLIT_TIME_C + endTime)
    var hbaseRDD = sparkContext.newAPIHadoopRDD(hBaseConf, classOf[XCXTableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    var dataRDD = hbaseRDD.flatMap(tuple2 => {
      val result = tuple2._2
      val rowKey = Bytes.toString(result.getRow)
      val rowKeys = StringUtils.split(rowKey, StatisticsConstant.KEY_SEPARATOR_C)
      if (rowKeys.length == 4) {
        var appid = rowKeys(2)
        var user_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("user_id")))
        var page_type = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("page_type")))
        var page_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("page_id")))
        var author_id = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("author_id")))
        var into_type = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("into_type")))
        var into_time = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("into_time")))
        var exit_time = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("exit_time")))
        if (into_time == null || "".equals(into_time)) {
          into_time = "-1"
        } else if (exit_time == null || "".equals(exit_time)) {
          exit_time = "-1"
        }
        val key = appid + StatisticsConstant.KEY_SEPARATOR_C + user_id + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id + StatisticsConstant.KEY_SEPARATOR_C +
          author_id + StatisticsConstant.KEY_SEPARATOR_C + into_type + StatisticsConstant.KEY_SEPARATOR_C + into_time + StatisticsConstant.KEY_SEPARATOR_C + exit_time
        List(key)
      } else {
        List()
      }
    }).cache()

    /**
      * 基于用户的浏览次数
      **/

    dataRDD.flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      if (keys.length == 8) {
        var appid = keys(0)
        var user_id = keys(1)
        var page_type = keys(2)
        var key = appid + StatisticsConstant.KEY_SEPARATOR_C + user_id + StatisticsConstant.KEY_SEPARATOR_C + page_type
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
        val updateRequest = BrowsePage.generateUserBrowsePageNum(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })

    /**
      * 基于用户计算浏览时长
      */
    dataRDD.flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      if (keys.length == 8) {
        var appid = keys(0)
        var user_id = keys(1)
        var into_time = keys(6)
        var exit_time = keys(7)
        if (!user_id.startsWith("tmp_") && !"-1".equals(into_time) && !"-1".equals(exit_time)) {
          var page_type = keys(2)
          var begin_time = java.lang.Long.parseLong(into_time)
          var end_time = java.lang.Long.parseLong(exit_time)
          var key = appid + StatisticsConstant.KEY_SEPARATOR_C + user_id + StatisticsConstant.KEY_SEPARATOR_C + page_type
          List((key, end_time - begin_time))
        } else {
          List()
        }
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
        val updateRequest = BrowsePage.generateUserBrowsePageTime(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })


    /**
      * 基于页面的浏览次数
      **/

    dataRDD.flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      if (keys.length == 8) {
        var appid = keys(0)
        var page_type = keys(2)
        var page_id = keys(3)
        var into_type = keys(5)
        var key = appid + StatisticsConstant.KEY_SEPARATOR_C + into_type + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
        var all_into_type_key = appid + StatisticsConstant.KEY_SEPARATOR_C + 0 + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
        List((key, 1L), (all_into_type_key, 1L))
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
        val updateRequest = BrowsePage.generatePageScanNum(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })

    /**
      * 基于页面的浏览人数
      */
    dataRDD.flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      if (keys.length == 8) {
        var appid = keys(0)
        var user_id = keys(1)
        var page_type = keys(2)
        var page_id = keys(3)
        var into_type = keys(5)
        var key = appid + StatisticsConstant.KEY_SEPARATOR_C + into_type + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id + StatisticsConstant.KEY_SEPARATOR_C + user_id
        var all_into_type_key = appid + StatisticsConstant.KEY_SEPARATOR_C + 0 + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id + StatisticsConstant.KEY_SEPARATOR_C + user_id
        List(key, all_into_type_key)
      } else {
        List()
      }
    }).distinct().flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      var appid = keys(0)
      var into_type = keys(1)
      var page_type = keys(2)
      var page_id = keys(3)
      var key = appid + StatisticsConstant.KEY_SEPARATOR_C + into_type + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
      List((key, 1L))
    }).reduceByKey((before, after) => {
      before + after
    }).foreachPartition(iterator => {
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext) {
        val tuple2 = iterator.next()
        val key = tuple2._1
        val value = tuple2._2
        val updateRequest = BrowsePage.generatePageScanPeopleNum(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })


    /**
      * 基于页面计算浏览时长
      **/
    dataRDD.flatMap(data => {
      val keys = StringUtils.split(data, StatisticsConstant.KEY_SEPARATOR_C)
      if (keys.length == 8) {
        var appid = keys(0)
        var into_time = keys(6)
        var exit_time = keys(7)
        if (!"-1".equals(into_time) && !"-1".equals(exit_time)) {
          var page_type = keys(2)
          var page_id = keys(3)
          var into_type = keys(5)
          var begin_time = java.lang.Long.parseLong(into_time)
          var end_time = java.lang.Long.parseLong(exit_time)
          var key = appid + StatisticsConstant.KEY_SEPARATOR_C + into_type + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
          var all_into_type_Key = appid + StatisticsConstant.KEY_SEPARATOR_C + 0 + StatisticsConstant.KEY_SEPARATOR_C + page_type + StatisticsConstant.KEY_SEPARATOR_C + page_id
          List((key, end_time - begin_time), (all_into_type_Key, end_time - begin_time))
        } else {
          List()
        }
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
        val updateRequest = BrowsePage.generatePageScanTime(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })

  }
}
