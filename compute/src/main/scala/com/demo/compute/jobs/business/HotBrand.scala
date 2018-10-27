package com.demo.compute.jobs.business

import java.sql.{Connection, PreparedStatement}
import java.util.Date

import com.alibaba.fastjson.JSON
import com.demo.compute.coms.Utils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object HotBrand {
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
    sqlCtx.udf.register("getBrandId", (value: String) => {
      val brandid = JSON.parseObject(value).getInteger("brandid")
      brandid match {
        case null => 0
        case _ => brandid
      }
    })

    val ds = sql(
      s"""
         |select a.brandId,count(a.brandId) as count
         |(select getBrandId(value) as brandId from source_date where date=${date}
         |and event='click' and value like '%opreate:click_brand%') as a
         |group by a.brandId
       """.stripMargin).as[(Int,Long)]

    def hotBrandDay(ds: Dataset[(Int, Long)]) = {
      ds.rdd.map(i => (i._1, i._2, date)).toDF("brandId", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotBrandDay", Utils.Jdbc.properties)
    }

    def hotProductWeek(dsnow: Dataset[(Int, Long)]) = {
      val sunday = Utils.weekdays(date)(0)
      val ds = sqlCtx.read.jdbc(Utils.Jdbc.url, "hotBrandWeek", Utils.Jdbc.properties)
        .selectExpr(
          s"""
             |select brandId,count from hotBrandWeek where date=${sunday}
          """.stripMargin).as[(Int, Long)]
      dsnow.rdd.subtract(ds.rdd).map(i => (i._1, i._2, sunday)).toDF("brandId", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotProductWeek", Utils.Jdbc.properties)

    }

    def hotProductMonth(dsnow: Dataset[(Long, Long)]) = {
      val numOne = Utils.monthdays(date)(0)
      val ds = sqlCtx.read.jdbc(Utils.Jdbc.url, "hotBrandMonth", Utils.Jdbc.properties)
        .selectExpr(
          s"""
             |select brandId,count from hotBrandMonth where date=${numOne}
          """.stripMargin).as[(Long, Long)]
      dsnow.rdd.subtract(ds.rdd).map(i => (i._1, i._2, numOne)).toDF("brandId", "count", "date")
        .write.jdbc(Utils.Jdbc.url, "hotBrandMonth", Utils.Jdbc.properties)

      ds.rdd.leftOuterJoin(dsnow.rdd).map(i => (i._1, i._2._2.getOrElse(0) + i._2._1))
        .map(i => (i._1, i._2, numOne))
        .foreachPartition(itr => {
          var connect: Connection = null
          var pstmt: PreparedStatement = null
          try {
            connect = Utils.Jdbc.conn
            connect.setAutoCommit(false)
            val sql = "insert into hotBrandMonth(brandId, count,date) values(?,?,?)"
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
