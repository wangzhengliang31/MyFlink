package com.wzl.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CheckpointAppS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    // 重启两次
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)))

    env.setStateBackend(new FsStateBackend("file:////Users/wangzhengliang/IdeaProjects/MyFlink/res/state"))
    // 由于ck，err异常之后重启，重启之后会累计之前状态，但程序重启后就没有了，因为state是在内存中
    env.socketTextStream("localhost", 9999)
        .map(x => {
          if(x.contains("err")) {
            throw new RuntimeException("BUG...")
          } else {
            x.toLowerCase
          }
        })
      .flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute(this.getClass.getSimpleName)
  }

}
