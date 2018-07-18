package com.cloudyoung.bigdata.miniprogram.statistics.recover

import com.cloudyoung.bigdata.common.hbase.{HBaseClient, HBaseContext}
import com.cloudyoung.bigdata.miniprogram.statistics.common.{StatisticsConstant, TableConstant}
import com.cloudyoung.bigdata.miniprogram.statistics.data.hbase.output.SaveDataHBaseImpl
import org.apache.hadoop.hbase.HBaseConfiguration

class Recover{

  def recover(date:String): Boolean ={
    val conf = HBaseContext.getHbaseConf
    val configration = HBaseConfiguration.create(conf)
    val savaData = new SaveDataHBaseImpl(TableConstant.result_module_day_stat, StatisticsConstant.family)



    true
  }

}
