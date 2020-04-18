package com.wzl.windows

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowsReduceAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map(x =>(1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))    // 滚动窗口
      .reduce((v1, v2) => {
        println("v1:" + v1 + "  v2:" + v2)
        (v1._1, v1._2 + v2._2)
      })
      .print()
      .setParallelism(1)

    env.execute("WindowsAppS")
  }
}
