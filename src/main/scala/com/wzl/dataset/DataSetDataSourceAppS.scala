package com.wzl.dataset

import com.wzl.bean.Person
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object DataSetDataSourceAppS {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //fromCollect(env)
    //fromTextFile(env)
    //fromTextDirectory(env)
    //fromCsvFile(env)
    fromRecursiveFiles(env)
  }

  // 从集合中读取
  def fromCollect(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }

  // 从文件中读取
  def fromTextFile(env: ExecutionEnvironment): Unit = {
    val path = "res/WC.txt"
    env.readTextFile(path).print()
  }

  // 从文件夹中读取
  def fromTextDirectory(env: ExecutionEnvironment): Unit = {
    val path = "res/"
    env.readTextFile(path).print()
  }

  case class MyCaseClass(name: String, age: Int)

  //  从Csv文件中读取
  def fromCsvFile(env: ExecutionEnvironment): Unit = {
    val path = "res/person.csv"
    import org.apache.flink.api.scala._
    //env.readCsvFile[(String, Int, String)](path, ignoreFirstLine = true).print()
    // env.readCsvFile[(String, Int)](path, ignoreFirstLine = true).print()
    // env.readCsvFile[(Int, String)](path, ignoreFirstLine = true, includedFields = Array(1, 2)).print()
    // env.readCsvFile[MyCaseClass](path, ignoreFirstLine = true, includedFields = Array(0,1)).print()    // Class
    env.readCsvFile[Person](path, ignoreFirstLine = true, pojoFields = Array("name", "age", "job")).print()   // POJO
  }

  // 多级文件夹读取
  def fromRecursiveFiles(env: ExecutionEnvironment): Unit = {
    val path = "res/"
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(path).withParameters(parameters).print()
  }

  // 从压缩文件读取，一般可以直接读
  def fromCompressionFile(env: ExecutionEnvironment): Unit ={
    val path = "res/tmp.gz"
    env.readTextFile(path).print()
  }
}
