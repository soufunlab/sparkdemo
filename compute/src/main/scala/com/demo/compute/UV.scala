package com.demo.compute

import java.sql
import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.demo.compute.UV.dateFormat
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-24 12:19 by 李浩（lihao@cloud-young.com）创建
  */
object UV {
  val dateFormat = new SimpleDateFormat("yyyy-M-d")
  var tableName = ""

  def main(args: Array[String]): Unit = {
    val uvTime = setUvTime(args: _*)
    val sqlTime = new java.sql.Date(uvTime(0).getTime)

    val conf = new SparkConf().setAppName(tableName)
//      .setMaster("local[1]")
    val sc = new SparkContext(conf)

    var filesPath = mutable.MutableList[String]()
    for (time <- uvTime) {
      filesPath += "hdfs://master:8020/user/root/test/%s/%s.txt".format(path(time), path(time))
    }
    val inputRdd = sc.textFile(filesPath.mkString(","))

        val allUv = inputRdd.map(_.split("\t")(1)).distinct().count()

//    val allUv = 1000
//    val cityUv = mutable.Map("北京" -> 100L, "南京" -> 100L)

    val cityUv = inputRdd.map(_.split("\t"))
      .map(i => (i(7) + "," + i(1), ""))
      .reduceByKey(_ + _).map(i => i._1.split(",")(0)).countByValue()

    val provinceUv = inputRdd.map(_.split("\t"))
      .map(i => (i(6) + "," + i(1), ""))
      .reduceByKey(_ + _).map(i => i._1.split(",")(0)).countByValue()

//    val provinceUv = mutable.Map("江苏" -> 100L, "河北" -> 100L)

    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into %s(action_time,province,city,count) values (?,?,?,?) ".format(tableName)
    try {
      conn = DriverManager.getConnection("jdbc:mysql://master:3306/compute?characterEncoding=utf8", "lihao", "123")

      {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setString(2, "")
        ps.setString(3, "")
        ps.setInt(4, allUv.toInt)
        ps.executeUpdate()
      }

      cityUv.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setString(2, "")
        ps.setString(3, data._1)
        ps.setInt(4, data._2.toInt)
        ps.executeUpdate()
      })

      provinceUv.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setDate(1, sqlTime)
        ps.setString(2, data._1)
        ps.setString(3, "")
        ps.setInt(4, data._2.toInt)
        ps.executeUpdate()
      })

    } catch {
      case ex: Exception => println(ex)
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def path(uvTime: java.util.Date) = dateFormat.format(uvTime)

  def setUvTime(args: String*): mutable.MutableList[java.util.Date] = {
    val runType = args(0)
    var list = mutable.MutableList[java.util.Date]()
    runType match {
      case "day" => {
        tableName = "uv_day"
        if (args.length == 2) {
          list += (dateFormat.parse(args(1)))
        } else {
          var c = Calendar.getInstance
          c.add(Calendar.DATE, -1)
          list += (c.getTime)
        }
      }
      case "week" => {
        tableName = "uv_week"
        var cal = Calendar.getInstance()
        if (args.length == 2) {
          var one = dateFormat.parse(args(1))
          cal.setTime(one)
          list += (one)
        } else {
          cal.add(Calendar.DATE, -7)
          cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
          list += (cal.getTime)
        }
        for (i <- 1 to 6) {
          cal.add(Calendar.DATE, 1)
          list += (cal.getTime)
        }
      }
      case "month" => {
        tableName = "uv_month"
        var cal = Calendar.getInstance()
        if (args.length == 2) {
          var one = dateFormat.parse(args(1))
          cal.setTime(one)
          list += (one)
        } else {
          cal.add(Calendar.DATE, -cal.get(Calendar.DAY_OF_MONTH))
          cal.set(Calendar.DAY_OF_MONTH, 1)
          list += (cal.getTime)
        }
        for (i <- 1 to (cal.getMaximum(Calendar.DAY_OF_MONTH) - 1)) {
          cal.add(Calendar.DATE, 1)
          list += (cal.getTime)
        }
      }
      case _ => throw new Exception
    }
    list
  }
}
