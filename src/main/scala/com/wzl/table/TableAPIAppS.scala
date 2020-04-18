package com.wzl.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TableAPIAppS {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    val input = env.readTextFile("res/person.csv" )
    val mapSet = input.filter(x => x.split(",")(0) != "name").map(x => {
      val splits = x.split(",")
      Access(splits(0).trim(), splits(1).trim().toInt, splits(2).trim(), splits(3).trim().toInt)
    })
    val table = tableEnv.registerDataSet("access", mapSet)

    //val tableAcc = tableEnv.scan("access").filter("name != 'zhangsan'").select("name, age")

    val tableAcc = tableEnv.scan("access").filter("name != 'zhangsan'").select("name, age").groupBy("name").aggregate("sum(age) as sum").select("name, sum")

    tableEnv.toDataSet[Row](tableAcc).print()
  }
}
