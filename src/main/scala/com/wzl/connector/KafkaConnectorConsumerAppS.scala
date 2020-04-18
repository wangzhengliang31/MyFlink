package com.wzl.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode

/**
 * val myConsumer = new FlinkKafkaConsumer08[String](...)
 * myConsumer.setStartFromEarliest()      // start from the earliest record possible
 * myConsumer.setStartFromLatest()        // start from the latest record
 * myConsumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
 * myConsumer.setStartFromGroupOffsets()  // the default behaviour
 */

object KafkaConnectorConsumerAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // ck机制常用参数
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.157.157:9092")
    properties.setProperty("group.id", "test")

    val data = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))
    data.print()

    env.execute("KafkaConnectorConsumerAppS")
  }

}
