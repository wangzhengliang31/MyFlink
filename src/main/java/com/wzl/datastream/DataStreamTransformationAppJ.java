package com.wzl.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataStreamTransformationAppJ {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //filterFunction(env);
        //unionFunction(env);
        splitSelectFunction(env);

        env.execute("DataStreamTransformationAppJ");
    }

    public static void filterFunction(StreamExecutionEnvironment env){
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunctionJ());

        data.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("received: " + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        }).print().setParallelism(1);
    }

    // 流合并
    public static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 = env.addSource(new CustomNonParallelSourceFunctionJ());
        DataStreamSource<Long> data2 = env.addSource(new CustomNonParallelSourceFunctionJ());

        data1.union(data2).print().setParallelism(1);
    }

    // 流拆分
    public static void splitSelectFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunctionJ());

        SplitStream<Long> splits = data.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> list = new ArrayList<String>();
                if(value %2 == 0){
                    list.add("even");    // 偶数
                } else {
                    list.add("odd") ;  // 奇数
                }
                return list;
            }
        });

        splits.select("odd").print().setParallelism(1);
    }
}
