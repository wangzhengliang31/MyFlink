package com.wzl.dataset

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

// 计数器
object DataSetCounterAppS {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("Hadoop", "Spark", "Flink", "pySpark", "Storm")

//    data.map(new RichMapFunction[String, Long]() {
//      var counter = 0l
//      override def map(in: String): Long = {
//        counter = counter + 1
//        println("counter: " + counter)
//        counter
//      }
//    }).setParallelism(3).print()    // 并行有问题

    val info = data.map(new RichMapFunction[String, String]() {

      // 定义计数器
      val counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        // 注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })
    info.writeAsText("res/sinkdir/counter", WriteMode.OVERWRITE).setParallelism(3)
    val jobResult = env.execute("DataSetCounterAppS")
    // 获取计数器
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")

    print(num)
  }

}
