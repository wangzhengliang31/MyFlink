package com.wzl.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowsProcessAppJ {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost", 9999);

        text.flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                String[] tokens = s.split(",");
                for(String token : tokens){
                    if(token.length() > 0){
                        collector.collect(new Tuple2<Integer, Integer>(1, Integer.parseInt(token)));
                    }
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))      // 滚动窗口
//                .timeWindow(Time.seconds(10), Time.seconds(5))      // 滑动窗口
                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
                        long count = 0;
                        System.out.println("==处理一次==");
                        for (Tuple2<Integer, Integer> in: elements) {
                            count++;
                        }
                        out.collect("Window: " + context.window() + "count: " + count);
                    }
                })
                .print()
                .setParallelism(1);

        env.execute("WindowsAppJ");
    }
}
