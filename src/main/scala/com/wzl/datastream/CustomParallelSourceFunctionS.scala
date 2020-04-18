package com.wzl.datastream

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
 * Flink数据源addsource
 * implementing the SourceFunction for non-parallel sources		//不可并行处理
 * implementing the ParallelSourceFunction interface
 * extending the RichParallelSourceFunction for parallel sources.
 */

class CustomParallelSourceFunctionS extends ParallelSourceFunction[Long]{
  var count = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning){
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
