package com.wzl.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
 * flink1.7
 */

object TableSQLAPIS_17 {
  def main(args: Array[String]): Unit = {
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    val tableEnv = TableEnvironment.getTableEnvironment(env)
//
//    val path = "res/person.csv"
//    val csv = env.readCsvFile[PersonLog](path, ignoreFirstLine = true)
//
//    val personTable = tableEnv.fromDataSet(csv)
//    tableEnv.registerTable("person", personTable)
//    val resultTable = tableEnv.sqlQuery("select name, age * salary from person")
//
//    tableEnv.toDataSet[Row](resultTable).print()
  }

  case class PersonLog(name:String, age:Int, job:String, salary:Int){

  }
}
