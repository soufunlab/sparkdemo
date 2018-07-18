package com.cloudyoung.bigdata.miniprogram.statistics.executor

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

abstract class SqlExecutor extends Executor {
  override def executor(sparkContext: SparkContext): Unit = ???
}
