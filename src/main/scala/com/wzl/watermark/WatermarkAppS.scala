package com.wzl.watermark

import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WatermarkAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   // Generating Timestamps 指定时间语义

    val text = env.socketTextStream("localhost", 9999)
    val data = text.map(x => {
      val arr = x.split(",")
      val code = arr(0)
      val time = arr(1).toLong
      (code, time)
    })

    val watermark = data.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrder = 10000L    //最大允许乱序时间

      var a : Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrder)
        a
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("timestamp:" + element._1 +","+ element._2 + "|" +format.format(element._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
        timestamp
      }
    })

    val  window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctions)
      .print()


    env.execute("WaterMarkAppS")
  }

  class WindowFunctions extends WindowFunction[(String, Long), (String, Int, String, String, String, String), String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int, String, String, String, String)]): Unit = {
      val list = input.toList.sortBy(_._2)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key, input.size, format.format(list.head._2), format.format(list.last._2), format.format(window.getStart), format.format(window.getEnd))
    }
  }
}
