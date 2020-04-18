package com.wzl.datastream

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object DataStreamTransformationAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //filterFunction(env)
    //unionFunction(env)
    splitSelectFunction(env)

    env.execute("DataStreamTransformationAppS")
  }

  def filterFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunctionS)

    data.map(x=>{
      println("received: " + x)
      x
    }).filter(_%2 == 0).print().setParallelism(1)
  }

  // 流合并
  def unionFunction(env:StreamExecutionEnvironment): Unit ={
    val data1 = env.addSource(new CustomNonParallelSourceFunctionS)
    val data2 = env.addSource(new CustomNonParallelSourceFunctionS)

    data1.union(data2).print().setParallelism(1)
  }

  // 流拆分
  def splitSelectFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunctionS)

    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if(value %2 == 0){
          list.add("even")    // 偶数
        } else {
          list.add("odd")   // 奇数
        }
        list
      }
    })

    splits.select("even").print().setParallelism(1)
  }
}
