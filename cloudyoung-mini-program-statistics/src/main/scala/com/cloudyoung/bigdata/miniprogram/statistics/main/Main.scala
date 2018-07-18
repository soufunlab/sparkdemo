package com.cloudyoung.bigdata.miniprogram.statistics.main
import java.util.Date

import com.cloudyoung.bigdata.miniprogram.statistics.job.DayJob

object Main{
  def main(args: Array[String]): Unit = {
    var date = new Date()
    var dayJob = new DayJob(date)
    var startTime = "20180601"
    var endTime = "20180702"
    var statTime = "20180601"
    dayJob.executor(startTime, endTime, statTime)
  }

}
