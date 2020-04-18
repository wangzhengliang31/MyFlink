package com.wzl.connector

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FileSystemSinkAppS {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val sink = new BucketingSink[String]("res/sinkdir/")
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter[String]())
    //sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    //sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins
    sink.setBatchRolloverInterval(2 * 1000); // this is 20 s

    data.print()
    data.addSink(sink)

    env.execute("FileSystemSinkAppS")
  }
}
