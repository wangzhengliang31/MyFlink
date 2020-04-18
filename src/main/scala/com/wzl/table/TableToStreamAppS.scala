package com.wzl.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row

object TableToStreamAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env.socketTextStream("localhost", 9999)
    val wordStream = stream.flatMap(_.split(",")).map(x => WC(x.toLowerCase(), 1))

    val table = tableEnv.fromDataStream(wordStream)
    tableEnv.registerTable("wc", table)

    // org.apache.flink.table.api.ValidationException: Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
    val res = tableEnv.sqlQuery("select word, count(*) from wc group by word")

    // tableEnv.toAppendStream[Row](table).print()
    tableEnv.toRetractStream[Row](res).print()

    env.execute(this.getClass.getSimpleName)
  }
}
