package com.cloudyoung.bigdata.miniprogram.statistics.executor

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

trait Executor {

  def executor(sqlContext:SQLContext, dataFrame:DataFrame)

  def executor(sparkContext: SparkContext)
}
