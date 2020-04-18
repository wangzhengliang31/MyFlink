package com.wzl.connector

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

object KafkaConnectorProducerAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost", 9999)

    val topic = "pktest"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.157.157:9092")

    val kafkaSink = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),properties)

    // 精确一次语义
//    val kafkaSink = new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    data.addSink(kafkaSink)

    env.execute("KafkaConnectorProducerAppS")
  }

}
