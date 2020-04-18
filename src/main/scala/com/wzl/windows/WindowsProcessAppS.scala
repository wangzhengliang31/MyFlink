package com.wzl.windows

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowsProcessAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    text.flatMap(_.split(","))
      .map(x =>(1, x.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))    // 滚动窗口
      .process(new MyProcessWindowFunction())
      .print()
      .setParallelism(1)

    env.execute("WindowsAppS")
  }
}

class MyProcessWindowFunction extends ProcessWindowFunction[(Int, Int), Object, Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[Object]): Unit = {
    var count = 0
    for (in <- elements) {
      count = count + 1
    }
    out.collect(s"Window ${context.window} count: $count")
  }
}
