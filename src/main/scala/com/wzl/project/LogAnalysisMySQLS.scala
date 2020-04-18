package com.wzl.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * flink 设置并行度：
 * env
 * addsource
 * addsink
 * print
 * ...
 */

object LogAnalysisMySQLS {
  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger("LogAnalysis")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.157.157:9092")
    properties.setProperty("group.id", "test-group")

    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    val data = env.addSource(consumer)
    val logData = data.map(x => {
      val splits = x.split("\t")
      val level = splits(2)
      var time = 0l
      try {
         time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(splits(3)).getTime()
      } catch {
        case e:Exception =>{
          logger.error("Time parse error: " + splits(3), e.getMessage)
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, time, domain, traffic)
    }).filter(_._2 != 0)
      .filter(_._1 == "E")
      .map(x => {
          (x._2, x._3, x._4)
      })

    val mysqlData = env.addSource(new MySQLSourceS)

    val connectData = logData.connect(mysqlData)
        .flatMap(new CoFlatMapFunction[(Long, String, Long), mutable.HashMap[String, String], String] {
          var userDomainMap = mutable.HashMap[String, String]()

          //log
          override def flatMap1(value: (Long, String, Long), out: Collector[String]): Unit = {
            val domain = value._2
            val user_id = userDomainMap.getOrElse(domain, "")

            print("user_id : " + user_id)

            out.collect(value._1 + "\t" + value._2 + "\t" + value._3 +  "\t" + user_id)
          }
          //mysql
          override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
            userDomainMap = value
          }
        })

    connectData.print()

    env.execute("LogAnalysis")
  }
}
