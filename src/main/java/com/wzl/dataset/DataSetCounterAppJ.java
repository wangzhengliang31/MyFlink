package com.wzl.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class DataSetCounterAppJ {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("Hadoop", "Spark", "Flink", "pySpark", "Storm");
        DataSet<String> info = data.map(new RichMapFunction<String, String>() {
            LongCounter counter = new LongCounter();

            @Override
            public String map(String s) throws Exception {
               counter.add(1);
               return s;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }
        });

        info.writeAsText("res/sinkdir/counter", FileSystem.WriteMode.OVERWRITE).setParallelism(3);
        JobExecutionResult jobResult = env.execute("DataSetCounterAppJ");
        // 获取计数器
        long num = jobResult.getAccumulatorResult("ele-counts-java");

        System.out.println(num);
    }
}
