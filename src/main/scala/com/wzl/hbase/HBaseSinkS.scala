package com.wzl.hbase

import org.apache.commons.lang.StringUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable.ListBuffer
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat

/**
 * 将数据写入HBase
 */

object HBaseSinkS {

  def convertToHBase(input: DataSet[(String, String, Int, String)]) = {
    input.map(new RichMapFunction[(String, String, Int, String), (Text, Mutation)] {
      val cf = "o".getBytes

      override def map(in: (String, String, Int, String)): (Text, Mutation) = {
        val id = in._1
        val name = in._2
        val age = in._3
        val city = in._4

        val text = new Text(id)
        val put = new Put(id.getBytes())
        if(StringUtils.isNotEmpty(name)){
          put.addColumn(cf, Bytes.toBytes("name"), Bytes.toBytes(name))
          put.addColumn(cf, Bytes.toBytes("age"), Bytes.toBytes(age.toString))
          put.addColumn(cf, Bytes.toBytes("city"), Bytes.toBytes(city))
        }

        (text, put)
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val students = ListBuffer[(String, String, Int, String)]()
    for(i <- 1 to 10){
      students.append(("" + i, "Test" + i, 20 + i, "beijing"))
    }

    val input = env.fromCollection(students)

    val res = convertToHBase(input)

    val cfg = HBaseConfiguration.create()
    cfg.set("hbase.zookeeper.quorum", "hadoop000")
    cfg.set("hbase.zookeeper.property.clientPort", "2181")
    cfg.set(TableOutputFormat.OUTPUT_TABLE, "student")
    cfg.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val jobRes = Job.getInstance(cfg)
    res.output(new HadoopOutputFormat(new TableOutputFormat[Text](), jobRes))

    env.execute("HBaseSinkS")
  }

}
