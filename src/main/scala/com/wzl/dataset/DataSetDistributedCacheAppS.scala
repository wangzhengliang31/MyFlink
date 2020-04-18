package com.wzl.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

// 分布式缓存
object DataSetDistributedCacheAppS {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 注册文件
    val path = "res/WC.txt"
    env.registerCachedFile(path, "dis-scala")

    val data = env.fromElements("Hadoop", "Spark", "Flink", "pySpark", "Storm")

    data.map(new RichMapFunction[String, String] {

      // 获取文件
      override def open(parameters: Configuration): Unit = {
        val dcFile = getRuntimeContext.getDistributedCache().getFile("dis-scala")
        val lines = FileUtils.readLines(dcFile)

        /**
         * 此时会出现一个异常，java和scala不兼容问题
         */
        import scala.collection.JavaConversions._
        for(i <- lines){
          println(i)
        }
      }
      override def map(in: String): String = {
        in
      }
    }).print()
  }

}
