package com.demo.spstream

import breeze.numerics.log
import com.cloudera.io.netty.handler.codec.string.StringDecoder
import kafka.message.MessageAndMetadata
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 说明：
  * 版权所有。
  *
  * @version 1.0 2018-9-4 17:12 by 李浩（lihao@cloud-young.com）创建
  */
object KafKaManager {
  /** **
    *
    * @param ssc          StreamingContext
    * @param kafkaParams  配置kafka的参数
    * @param zkClient     zk连接的client
    * @param zkOffsetPath zk里面偏移量的路径
    * @param topics       需要处理的topic
    * @return InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, String],
                        zkClient: ZkClient,
                        zkOffsetPath: String,
                        topics: Set[String]): InputDStream[(String, String)] = {
    //目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    val zkOffsetData = KafkaOffsetManager.readOffsets(zkClient, zkOffsetPath, topics.last)

    val kafkaStream = zkOffsetData match {
      case None => //如果从zk里面没有读到偏移量，就说明是系统第一次启动
        //使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(lastStopOffset) =>

        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)

        //使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
          ssc, kafkaParams, lastStopOffset, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }
    kafkaStream //返回创建的kafkaStream
  }

  /** **
    * 保存每个批次的rdd的offset到zk中
    *
    * @param zkClient     zk连接的client
    * @param zkOffsetPath 偏移量路径
    * @param rdd          每个批次的rdd
    */
  def saveOffsets(zkClient: ZkClient, zkOffsetPath: String, rdd: RDD[_]): Unit = {
    //转换rdd为Array[OffsetRange]
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //转换每个OffsetRange为存储到zk时的字符串格式 :  分区序号1:偏移量1,分区序号2:偏移量2,......
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
    log.debug(" 保存的偏移量：  " + offsetsRangesStr)
    //将最终的字符串结果保存到zk里面
    ZkUtils.updatePersistentPath(zkClient, zkOffsetPath, offsetsRangesStr)
  }
}
