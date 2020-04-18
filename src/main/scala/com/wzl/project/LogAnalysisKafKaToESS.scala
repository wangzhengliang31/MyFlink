package com.wzl.project

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

object LogAnalysisKafKaToESS {
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

    //logData.print().setParallelism(1)

    val result = logData.assignTimestampsAndWatermarks( new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

      val maxOutOfOrderness = 10000L
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1)
        .window(TumblingEventTimeWindows.of(Time.seconds(60)))
        .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
            val domain = key.getField(0).toString
            var sum = 0l
            var time = 0l
            val iterator = input.iterator
            while(iterator.hasNext) {
              val next = iterator.next()
              sum += next._3
              time = next._1
            }
            out.collect((time.toString, domain, sum))
          }
        })

    val httpHosts = new java.util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("192.168.157.157", 9200, "http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
      httpHosts,
      new ElasticsearchSinkFunction[(String, String, Long)] {
        def createIndexRequest(element: (String, String, Long)): IndexRequest = {
          val json = new java.util.HashMap[String, Any]
          json.put("time", element._1)
          json.put("domain", element._2)
          json.put("traffic", element._3)

          val id = element._1 + "-" + element._2

          return Requests.indexRequest()
            .index("my-index")
            .`type`("my-type")
            .id(id)
            .source(json)
        }

        override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(t))
        }
      }
    )

    esSinkBuilder.setBulkFlushMaxActions(1)

    result.addSink(esSinkBuilder.build)

    env.execute("LogAnalysis")
  }
}
