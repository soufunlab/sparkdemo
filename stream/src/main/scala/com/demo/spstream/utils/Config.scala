package com.demo.spstream.utils

import java.io.FileInputStream
import java.util.Properties

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-7-18 14:58 by 李浩（lihao@cloud-young.com）创建
  */
object Config extends Serializable {

  val props = new Properties()
  val path = Thread.currentThread().getContextClassLoader.getResource("config.properties").getPath
  props.load(new FileInputStream(path))
//    props.load(new FileInputStream("config.properties"))

  val timeInterval = get("timeInterval")
  val brokerList = get("brokerList")
  val zkQuorum = get("zkQuorum")
  val topic = get("topic")
  val groupId = get("groupId")
  var produceTopic = get("produceTopic")

  def get(key: String) = {
    props.getProperty(key)
  }
}
