package com.longhchen

import org.apache.flink.streaming.api.scala._

/**
 * Create by longhchen on  2020-07-12 9:55
 */
object ScoketScalaStreamWordCount {
  def main(args: Array[String]): Unit = {
    
    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[String] = env.socketTextStream("localhost", 11111)

    val DStream: DataStream[(String, Int)] = stream.flatMap(x => x.split("\\W+")).map(x => (x, 1)).keyBy(0).sum(1)

    DStream.print()

    env.execute("scala wordcount")
  }

}
