package com.cloudyoung.bigdata.miniprogram.statistics.job

import java.util.Date

import com.cloudyoung.bigdata.common.Constant
import com.cloudyoung.bigdata.miniprogram.statistics.executor._
import org.apache.spark.{SparkConf, SparkContext}

class DayJob(val date: Date) {

  def executor(startTime: String, endTime: String, statTime: String): Unit = {

    val cluster = "devbmaster:2181"
    val timeout = 6000
    Constant.init(cluster, timeout)

    val sparkConf = new SparkConf().setAppName("HBaseTest");
    val sparkContext = new SparkContext(sparkConf)
    val userEventExecutor = new UserEventExecutor(startTime, endTime, "lovecar_dev:t_user_event", statTime, "lovecar_dev:stat_user_detail_day")
    val exposureExecutor = new ExposureExecutor(startTime, endTime, "lovecar_dev:t_message_exposure", statTime, "lovecar_dev:stat_user_detail_day")
    val hotNavigateClickExecutor = new HotNavigateClickExecutor(startTime, endTime, "lovecar_dev:t_hot_navigate_click", statTime, "lovecar_dev:stat_user_detail_day")
    val userBrowseExecutor = new UserBrowseExecutor(startTime, endTime, "lovecar_dev:t_user_brower", statTime, "lovecar_dev:stat_user_detail_day")
    val userBehaviorEventExecutor = new UserBehaviorEventExecutor(startTime, endTime, "lovecar_dev:t_user_behavior_event", statTime, "lovecar_dev:stat_user_detail_day")
    val userLaunchExecutor = new UserLaunchExecutor(startTime, endTime, "lovecar_dev:t_user_launch", statTime, "lovecar_dev:stat_user_detail_day")

//    userEventExecutor.executor(sparkContext)
//    userBehaviorEventExecutor.executor(sparkContext)
//    userBrowseExecutor.executor(sparkContext)
//    hotNavigateClickExecutor.executor(sparkContext)
    exposureExecutor.executor(sparkContext)
//    userLaunchExecutor.executor(sparkContext)


  }


}
