package com.wzl.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 求平均数：每到达两个元素就求平均数
 *
 */

object KeyedStateAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List((1,3), (1,5), (1,7), (1,4), (1,2)))
      .keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Int, Int), (Int, Int)] {
        var state:ValueState[(Int, Int)] = _

        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getState(new ValueStateDescriptor[(Int, Int)]("avg", createTypeInformation[(Int, Int)]))
        }

        override def flatMap(in: (Int, Int), collector: Collector[(Int, Int)]): Unit = {
          val tmpState:(Int, Int) = state.value()

          val currentState = if(null != tmpState) {
            tmpState
          } else {
            (0, 0)
          }

          val newState = (currentState._1 + 1, currentState._2 + in._2)

          state.update(newState)

          if(newState._1 >= 2) {
            collector.collect((in._1, newState._2 / newState._1))
            state.clear()
          }
        }
      }).print()

    env.execute(this.getClass.getSimpleName)
  }
}
