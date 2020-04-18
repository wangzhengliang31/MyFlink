package com.wzl.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Flink数据源addsource
 * implementing the SourceFunction for non-parallel sources		//不可并行处理
 * implementing the ParallelSourceFunction interface
 * extending the RichParallelSourceFunction for parallel sources.
 */

public class CustomRichParallelSourceFunctionJ extends RichParallelSourceFunction<Long> {
    boolean isRunning = true;
    long count = 1;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (true){
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
