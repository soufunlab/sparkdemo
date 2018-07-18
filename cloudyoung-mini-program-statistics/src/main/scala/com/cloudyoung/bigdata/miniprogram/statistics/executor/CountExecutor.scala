

package com.cloudyoung.bigdata.miniprogram.statistics.executor

import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output.SaveDataHBaseImpl
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}



class CountExecutor  extends  SqlExecutor {

  override def executor(sqlContext:SQLContext, dataFrame :DataFrame ): Unit = {
    val df = sqlContext.sql("select style_id, serial_id, province_id, city_id, deal_id from record")
    df.show()

    df.foreachPartition( iter => {
      val saveData = new SaveDataHBaseImpl("xcx_record", "info")
      while (iter.hasNext){
        val tuple = iter.next()
        val style_id = tuple.get(1)
        saveData.saveRowData("00:20180426:43", "style_id", style_id.toString)
      }
      saveData.closeTable()
    })

    //停留时长
    val df1 = sqlContext.sql("select trace_id from record ")
    df1.show()

    df1.foreachPartition(iter => {
      val saveData = new SaveDataHBaseImpl("xcx_record","info")
      while(iter.hasNext){
        val tuple = iter.next()
        val trace_id = tuple.get(1)
        saveData.saveRowData("00:20180426:44","trace_id",trace_id.toString)

      }
      saveData.closeTable()
    })

    //分享次数 呼叫顾问人数
    val df2 = sqlContext.sql("select sum(event_page_action_code) from record")
    df2.show()

    df2.foreachPartition(iter =>{
      val saveData = new SaveDataHBaseImpl("xcx_record","info")
      while(iter.hasNext){
        val tuple = iter.next()
        val event_page_action_code = tuple.get(1)
        saveData.saveRowData("00:20180426:45","event_page_action_code",event_page_action_code.toString)

      }
      saveData.closeTable()
    })

   
  }

}


