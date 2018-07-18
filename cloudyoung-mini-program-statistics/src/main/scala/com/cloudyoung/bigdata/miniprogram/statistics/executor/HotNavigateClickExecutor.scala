package com.cloudyoung.bigdata.miniprogram.statistics.executor
import com.cloudyoung.bigdata.common.hbase.HBaseContext
import com.cloudyoung.bigdata.miniprogram.statistics.common.StatisticsConstant
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.generate.HotNavigate
import com.cloudyoung.bigdata.miniprogram.statistics.data.elasticsearch.ouput.ElasticSearchClient
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.input.format.XCXTableInputFormat
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

class HotNavigateClickExecutor(startTime: String, endTime: String, tableName: String, statTime: String, savaTable: String) extends CoreExecutor{
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
      if(rowKeys.length == 7){
        var appid = rowKeys(2)
        var hot_type = rowKeys(3)
        var page_type = rowKeys(4)
        var page_id = rowKeys(5)
        var user_id = rowKeys(6)
        var key = appid + StatisticsConstant.KEY_SEPARATOR_C + hot_type + StatisticsConstant.KEY_SEPARATOR_C + user_id
        List((key, 1L))
      } else {
        List()
      }
    }).reduceByKey((before, after) =>{
      before + after
    }).foreachPartition(iterator =>{
      val elasticSearchClient = new ElasticSearchClient()
      while (iterator.hasNext) {
        val tuple2 = iterator.next();
        val key = tuple2._1
        val value = tuple2._2
        val updateRequest = HotNavigate.generateHotNavigateClick(statTime, key, value)
        if (updateRequest != null) {
          elasticSearchClient.add(updateRequest)
        }
      }
      elasticSearchClient.close()
    })
  }
}
