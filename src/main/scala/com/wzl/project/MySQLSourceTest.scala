package com.wzl.project

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object MySQLSourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.addSource(new MySQLSourceS).setParallelism(1)
    data.print()

    env.execute("MySQLSourceTest")
  }
}
