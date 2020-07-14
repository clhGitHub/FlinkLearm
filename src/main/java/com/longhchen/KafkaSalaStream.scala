package com.longhchen

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * Create by longhchen on  2020-07-12 10:15
 */
object KafkaSalaStream {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置kafka参数
    val properties = new Properties()

    properties.setProperty("bootstrap.servers","hadoop102:9092")
    //properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("flinktest", new SimpleStringSchema(), properties))

    stream.print()

    env.execute()


  }

}
