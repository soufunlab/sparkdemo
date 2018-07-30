package com.demo.compute

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-24 12:19 by 李浩（lihao@cloud-young.com）创建
  */
object UV_day {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val uvTime = uvTime(args(0))
    val conf = new SparkConf().setAppName("UV_day")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile("hdfs://master:8020/user/root/test/s%/s%.txt".format(path(uvTime), path(uvTime)))

  }

  def path(uvTime: Date) = dateFormat.format(uvTime).split(" ")(0)

  def uvTime(time: String) = {
    if (!time.isEmpty) {
      dateFormat.parse(time)
    } else {
      var c = Calendar.getInstance
      c.add(Calendar.DATE, -1)
      c.getTime
    }
  }
}
