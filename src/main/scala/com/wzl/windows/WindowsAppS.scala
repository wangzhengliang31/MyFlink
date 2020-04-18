package com.wzl.windows

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object WindowsAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))    // 滚动窗口
//      .timeWindow(Time.seconds(10), Time.seconds(5))    // 滑动窗口
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("WindowsAppS")
  }
}
