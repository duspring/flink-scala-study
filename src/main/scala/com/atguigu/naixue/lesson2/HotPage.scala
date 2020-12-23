package com.atguigu.naixue.lesson2

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


// 输入数据样例类
case class ApacheLogEvent( ip: String, // IP地址
                           userId: String, // 用户ID
                           eventTime: Long, // 用户点击广告时间
                           method: String, // 请求方式
                           url: String) // 请求的URL

// 窗口聚合结果样例类
case class UrlViewCount( url: String, // 请求的URL
                         windowEnd: Long, // 所属窗口
                         count: Long ) // 点击的次数



/**
  * @author: spring du
  * @description: 热门页面统计
  * @date: 2020/12/10 16:11
  */
object HotPage {

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.readTextFile("/input/")  // 读取数据
//      .map() // 使用面向对象的思想，对数据进行解析
//      .assignTimestampsAndWatermarks() // 设置水位，允许数据迟到10秒
//      .keyBy() // 根据请求页面进行分组
//      .timeWindow(Time.minutes(5), Time.seconds(5)) // 设置窗口
//      .aggregate() // 窗口URL进行统计
//      .keyBy() // 按照窗口进行分组
//      .process() // 实现排序的逻辑
//      .print() // 打印输出

    env.execute("hot page count")
  }

}
