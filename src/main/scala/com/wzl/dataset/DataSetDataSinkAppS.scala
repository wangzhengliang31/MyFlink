package com.wzl.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode


object DataSetDataSinkAppS {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = 1 to 10
    val test = env.fromCollection(data).setParallelism(2)   // 并行度改变输出,1为txt,2为文件夹下两个文本文件

    val path = "res/sinkdir/test.txt"
    test.writeAsText(path, WriteMode.OVERWRITE)

    env.execute("DataSetDataSinkAppS")
  }
}
