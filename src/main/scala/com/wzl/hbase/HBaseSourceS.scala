package com.wzl.hbase

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.configuration.Configuration

import org.apache.flink.api.scala._
/**
 * 将数据读取HBase
 */

object HBaseSourceS {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val cf = "o".getBytes()

    var res = env.createInput(new TableInputFormat[Tuple4[String, String, Int,String]]{

      def createTable(): Unit ={
        val cfg = HBaseConfiguration.create()
        cfg.set("hbase.zookeeper.quorum", "hadoop000")
        cfg.set("hbase.zookeeper.property.clientPort", "2181")
        new HTable(cfg, getTableName)

      }

      override def configure(parameters: Configuration): Unit = {
        val table = createTable()
        if(table != null){
          scan = getScanner
        }
      }

      override def getScanner: Scan = {
        val scan = new Scan()
        scan.addFamily(cf)
        scan
      }

      override def getTableName: String = {
        "student"
      }

      override def mapResultToTuple(result: Result): Tuple4[String, String, Int,String] = {
        new Tuple4[String, String, Int,String](Bytes.toString(result.getRow),
          Bytes.toString(result.getValue(cf, "name".getBytes())),
          Bytes.toString(result.getValue(cf, "age".getBytes())).toInt,
          Bytes.toString(result.getValue(cf, "city".getBytes())))
      }
    })

    res.print()

    env.execute("HBaseSinkS")
  }

}
