package com.wzl.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamDataSourceAppJ {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //socketFunction(env);
        //customNonParallelSourceFunction(env);
        //customParallelSourceFunction(env);
        customRichParallelSourceFunction(env);

        env.execute("DataStreamDataSourceAppJ");
    }

    public static void socketFunction(StreamExecutionEnvironment env) throws Exception{
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);

        data.print();
        //env.execute("DataStreamDataSourceAppJ");
    }

    public static void customNonParallelSourceFunction(StreamExecutionEnvironment env) throws Exception{
        DataStreamSource<Long> data = env.addSource(new CustomNonParallelSourceFunctionJ());    //并行度只能为1
        data.print();
    }

    public static void customParallelSourceFunction(StreamExecutionEnvironment env) throws Exception{
        DataStreamSource<Long> data = env.addSource(new CustomParallelSourceFunctionJ()).setParallelism(3);
        data.print();
    }

    public static void customRichParallelSourceFunction(StreamExecutionEnvironment env) throws Exception{
        DataStreamSource<Long> data = env.addSource(new CustomParallelSourceFunctionJ()).setParallelism(3);
        data.print();
    }
}
