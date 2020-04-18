package com.wzl.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DataSetDistributedCacheAppJ {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String path = "res/WC.txt";
        env.registerCachedFile(path, "dis-java");

        DataSource<String> data = env.fromElements("Hadoop", "Spark", "Flink", "pySpark", "Storm");

        data.map(new RichMapFunction<String, String>() {
            List<String> lists = new ArrayList<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("dis-java");
                List<String> lines = FileUtils.readLines(file);

                for(String line : lines){
                    lists.add(line);
                    System.out.println(line);
                }
            }

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).print();
    }
}
