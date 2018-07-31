package com.demo.compute

import java.sql
import java.sql.{Connection, DriverManager, PreparedStatement, Date}
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
    val uvTime = setUvTime(args(0))
    val sqlTime = new java.sql.Date(uvTime.getTime)

    val conf = new SparkConf().setAppName("UV_day")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile("hdfs://master:8020/user/root/test/s%/s%.txt".format(path(uvTime), path(uvTime)))

    val allUv = inputRdd.map(_.split("\t")(1)).map(i => (i, 1)).reduceByKey(_ + _).collect()

    val cityUv = inputRdd.map(_.split("\t"))
      .map(i => (i(7) + "," + i(1), ""))
      .reduceByKey(_ + _).map(i => i._1.split(",")(0)).countByValue()

    val provinceUv = inputRdd.map(_.split("\t"))
      .map(i => (i(6) + "," + i(1), ""))
      .reduceByKey(_ + _).map(i => i._1.split(",")(0)).countByValue()

    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into uv_day(action_time,province,city,count) values (?,?,?,?) "
    try {
      conn = DriverManager.getConnection("jdbc:mysql://master:3306/compute", "lihao", "123")

      allUv.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setInt(4, data._2)
        ps.executeUpdate()
      })

      cityUv.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setString(3, data._1)
        ps.setInt(4, data._2.toInt)
        ps.executeUpdate()
      })

      provinceUv.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setString(2, data._1)
        ps.setInt(4, data._2.toInt)
        ps.executeUpdate()
      })

    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def path(uvTime: java.util.Date) = dateFormat.format(uvTime).split(" ")(0)

  def setUvTime(time: String) = {
    if (!time.isEmpty) {
      dateFormat.parse(time)
    } else {
      var c = Calendar.getInstance
      c.add(Calendar.DATE, -1)
      c.getTime
    }
  }
}
