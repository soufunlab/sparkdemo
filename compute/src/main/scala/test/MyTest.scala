package test

import java.util.Date

import com.demo.compute.coms.{LogObj, Utils}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 说明：
  * 版权所有。
  * 日次均使用时长
  *
  * @version 1.0 2018-9-7 14:36 by 李浩（lihao@cloud-young.com）创建
  */
object MyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pv-day")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scores = sc.parallelize(List(("chinese", 88.0), ("chinese", 90.5), ("math", 60.0), ("math", 87.0)))
    scores.mapValues((_, 1)).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(i => i._1 / i._2).collect()
  }
}
