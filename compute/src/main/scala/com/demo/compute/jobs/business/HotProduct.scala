package com.demo.compute.jobs.business

import java.sql.{Connection, PreparedStatement}
import java.util.Date

import com.demo.compute.coms.Utils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.uncommons.maths.statistics.DataSet


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object HotProduct {
  var date: Date = null

  def main(args: Array[String]): Unit = {
    this.date = Utils.executeTime(args)
    val conf = new SparkConf().setAppName("deep-perday")
    //          .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new HiveContext(sc)

    import sqlCtx.implicits._
    import sqlCtx.sql

    val pattern = """productid=(\d+)""".r
    sqlCtx.udf.register("getProductid", (value: String) => {
      pattern.findFirstMatchIn(value) match {
        case Some(s) => s.group(0).toInt
        case None => 0
      }
    })

    val df = sql(
      s"""
         |select product,count from
         |(select a.product,count(a.product) as count from
         |(select getProductid(pageurl) as product from source_data where date=${date}
         |and pageurl like '%page6%') a
         |where a.product!=0 group by a.product) as b
         |order by b.count desc
      """.stripMargin).as[(Long, Long)]


    def hotProductDay(ds: Dataset[(Long, Long)]) = {
      ds.rdd.map(i => (i._1, i._2, date)).toDF("product", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotProductDay", Utils.Jdbc.properties)
    }

    def hotProductWeek(dsnow: Dataset[(Long, Long)]) = {
      val sunday = Utils.weekdays(date)(0)
      val ds = sqlCtx.read.jdbc(Utils.Jdbc.url, "test", Utils.Jdbc.properties)
        .selectExpr(
          s"""
             |select product,count from test where date=${sunday}
          """.stripMargin).as[(Long, Long)]
      dsnow.rdd.subtract(ds.rdd).map(i => (i._1, i._2, sunday)).toDF("product", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotProductWeek", Utils.Jdbc.properties)

      ds.rdd.leftOuterJoin(dsnow.rdd).map(i => (i._1, i._2._2.getOrElse(0) + i._2._1))
        .map(i => (i._1, i._2, sunday))
        .toDF("product", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotProductWeek", Utils.Jdbc.properties)
    }

    def hotProductMonth(dsnow: Dataset[(Long, Long)]) = {
      val numOne = Utils.monthdays(date)(0)
      val ds = sqlCtx.read.jdbc(Utils.Jdbc.url, "hotProductMonth", Utils.Jdbc.properties)
        .selectExpr(
          s"""
             |select product,count from hotProductMonth where date=${numOne}
          """.stripMargin).as[(Long, Long)]
      dsnow.rdd.subtract(ds.rdd).map(i => (i._1, i._2, numOne)).toDF("product", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotProductMonth", Utils.Jdbc.properties)

      ds.rdd.leftOuterJoin(dsnow.rdd).map(i => (i._1, i._2._2.getOrElse(0) + i._2._1))
        .map(i => (i._1, i._2, numOne))
        .foreachPartition(itr => {
          var connect: Connection = null
          var pstmt: PreparedStatement = null
          try {
            connect = Utils.Jdbc.conn
            connect.setAutoCommit(false)
            val sql = "insert into hotProductMonth(product, count,date) values(?,?,?)"
            pstmt = connect.prepareStatement(sql)
            for (ele <- itr) {
              pstmt.setLong(1, ele._1)
              pstmt.setLong(2, ele._2)
              pstmt.setDate(3, new java.sql.Date(ele._3.getTime))
              pstmt.addBatch()
            }
            pstmt.executeBatch()
            connect.commit()
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            if (pstmt != null) {
              pstmt.close()
            }
            if (connect != null) {
              connect.close()
            }
          }
        })
    }

  }


}
