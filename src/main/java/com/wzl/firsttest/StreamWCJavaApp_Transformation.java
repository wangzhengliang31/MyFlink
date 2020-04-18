package com.wzl.firsttest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink实时应用程序
 *
 * 数据源自于socket  nc -lk 9999
 */

public class StreamWCJavaApp_Transformation {
    public static void main(String[] args) throws Exception{

        int port = 0;
        ParameterTool tool = ParameterTool.fromArgs(args);
        try {
            port = tool.getInt("port");
        } catch (Exception e) {
            System.err.println("port is no define, use default port 9999.");
            port = 9999;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.socketTextStream("localhost", port);

//        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] tokens = value.toLowerCase().split(" ");
//                for(String token : tokens){
//                    if(token.length()  > 0){
//                        collector.collect(new Tuple2<String, Integer>(token, 1));
//                    }
//                }
//            }
//        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);

        //text.flatMap(new MyFlatMapFunction())   // 实现一个接口
        text.flatMap(new RichFlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> collector) throws Exception {
                String[] tokens = value.toLowerCase().split(" ");
                for (String token : tokens)
                    if (token.length() > 0) {
                        collector.collect(new WC(token, 1));
                    }
            }
        })
                // .keyBy("world") // 直接指定key或使用keyselector
        .keyBy(new KeySelector<WC, String>() {
            @Override
            public String getKey(WC wc) throws Exception {
                return wc.world;
            }
        }).timeWindow(Time.seconds(5)).sum("count").print().setParallelism(1);


        env.execute("StreamWCJavaApp");
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, WC> {
        @Override
        public void flatMap(String value, Collector<WC> collector) throws Exception {
            String[] tokens = value.toLowerCase().split(" ");
            for (String token : tokens)
                if (token.length() > 0) {
                    collector.collect(new WC(token, 1));
                }
        }
    }

    public static class WC {
        private String world;
        private int count;

        public WC() {
        }

        public WC(String world, int count) {
            this.world = world;
            this.count = count;
        }

        public String getWorld() {
            return world;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void setWorld(String world) {
            this.world = world;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "world='" + world + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
