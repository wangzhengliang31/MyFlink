package com.wzl.datastream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
 * Flink数据源addsource
 * implementing the SourceFunction for non-parallel sources		//不可并行处理
 * implementing the ParallelSourceFunction interface
 * extending the RichParallelSourceFunction for parallel sources.
 */

object DataStreamDataSourceAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //socketFunction(env)
    //customNonParallelSourceFunction(env)
    //customParallelSourceFunction(env)
    customRichParallelSourceFunction(env)

    env.execute("DataStreamDataSourceAppS")
  }

  def socketFunction(env: StreamExecutionEnvironment): Unit ={
    val data = env.socketTextStream("localhost", 9999)
    data.print()

    //env.execute("DataStreamDataSourceAppS")
  }

  def customNonParallelSourceFunction(env: StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunctionS)   // 并行度只能为1，否则报错
    data.print()
  }

  def customParallelSourceFunction(env: StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomParallelSourceFunctionS).setParallelism(3)
    data.print()
  }

  def customRichParallelSourceFunction(env: StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomRichParallelSourceFunctionS).setParallelism(3)
    data.print()
  }
}
