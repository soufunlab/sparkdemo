package com.demo.spstream.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import Config
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-5 11:16 by 李浩（lihao@cloud-young.com）创建
  */
object Utils {

  val event_register = "register"
  val event_load = "load"
  val event_view = "view"
  val event_exit = "exit"
  val event_error = "error"

  def hbaseConn = {
    val hbaseConf = new JobConf(HBaseConfiguration.create())
    hbaseConf.set("hbase.zookeeper.quorum", Config.zkQuorum)
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.setOutputFormat(classOf[TableOutputFormat])
    ConnectionFactory.createConnection(hbaseConf)
  }

  def getHour() = {
    val calendar = Calendar.getInstance()
    var simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val min = calendar.get(Calendar.MINUTE)
    if (min <= 30) {
      simpleDateFormat.format(calendar.getTime)
    } else {
      calendar.add(Calendar.HOUR, 1)
      simpleDateFormat.format(calendar.getTime)
    }
  }

  case class Entity(val time: String, val openid: String, val traceid: String, val sourceurl: String, val pageurl: String, val staytime: String, val province: String, val city: String, val event: String, val device: String, val os: String)

}
