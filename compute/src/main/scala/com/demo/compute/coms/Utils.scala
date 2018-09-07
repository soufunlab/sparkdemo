package com.demo.compute.coms

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.demo.compute.jobs.MeanTimeDay.date
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-7 14:49 by 李浩（lihao@cloud-young.com）创建
  */
object Utils {

  val event_register = "register"
  val event_load = "load"
  val event_view = "view"
  val event_exit = "exit"
  val event_error = "error"

  val dfsFormart = new SimpleDateFormat("yyyy-M-d")

  def executeTime(args: Array[String]) = {
    var date: Date = null
    if (args != null && args.length > 0 && StringUtils.isNotEmpty(args(0))) {
      date = dfsFormart.parse(args(0))
    } else {
      val cl = Calendar.getInstance()
      cl.add(Calendar.DATE, -1)
      date = cl.getTime
    }
    date
  }

  def hadoopConf() = {
    val config = new Configuration()
    config.set("dfs.nameservices", "nameservice1")
    config.set("dfs.ha.namenodes.nameservice1", "namenode46,namenode64")
    config.set("dfs.namenode.rpc-address.nameservice1.namenode46", "master:8020")
    config.set("dfs.namenode.rpc-address.nameservice1.namenode64", "slave1:8020")
    config.set("fs.defaultFS", "hdfs://nameservice1")
    config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    config.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    config
  }

  def setHadoopConf(conf: Configuration) = {
    conf.set("dfs.nameservices", "nameservice1")
    conf.set("dfs.ha.namenodes.nameservice1", "namenode46,namenode64")
    conf.set("dfs.namenode.rpc-address.nameservice1.namenode46", "master:8020")
    conf.set("dfs.namenode.rpc-address.nameservice1.namenode64", "slave1:8020")
    conf.set("fs.defaultFS", "hdfs://nameservice1")
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
  }

  def dfs_date(date: Date) = {
    dfsFormart.format(date)
  }

  def longTime(time: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(time).getTime
  }

  val hbaseConn = ConnectionFactory.createConnection(hbaseConf)

  def hbaseConf = {
    val hbaseConf = new JobConf(HBaseConfiguration.create())
    hbaseConf.set("hbase.zookeeper.quorum", Config.zkQuorum)
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.setOutputFormat(classOf[TableOutputFormat])
    hbaseConf
  }

  def hbaseDay(time: Date) = {
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(time)
  }

  def weekdays(time: Date) = {
    var cal = Calendar.getInstance()
    cal.setTime(time)
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)

    val reL = new ListBuffer[Date]()
    while (cal.getTime.getTime - time.getTime <= 0) {
      reL += cal.getTime
      cal.add(Calendar.DATE, 1)
    }
    reL
  }

  def monthdays(time: Date) = {
    var cal = Calendar.getInstance()
    cal.setTime(time)
    cal.set(Calendar.DAY_OF_MONTH, 1)

    val reL = new ListBuffer[Date]()
    while (cal.getTime.getTime - time.getTime <= 0) {
      reL += cal.getTime
      cal.add(Calendar.DATE, 1)
    }
    reL
  }
}
