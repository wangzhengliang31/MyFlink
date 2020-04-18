package com.wzl.table

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object StreamTableAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val input = env.readTextFile("res/person.csv" )
    val mapStream = input.filter(x => x.split(",")(0) != "name").map(x => {
      val splits = x.split(",")
        Access(splits(0).trim(), splits(1).trim().toInt, splits(2).trim(), splits(3).trim().toInt)
    })
    val table = tableEnv.fromDataStream(mapStream)

    val res = table.select("name").filter("name == 'lisi'")
    tableEnv.toAppendStream[Row](res).print().setParallelism(1)

    env.execute("StreamSQLAPPS")
  }

}
