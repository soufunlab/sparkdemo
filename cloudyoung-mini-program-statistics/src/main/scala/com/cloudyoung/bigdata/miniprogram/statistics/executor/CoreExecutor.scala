package com.cloudyoung.bigdata.miniprogram.statistics.executor

import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class CoreExecutor extends Executor  with Serializable{
  override def executor(sqlContext:SQLContext, dataFrame:DataFrame): Unit = ???
}
