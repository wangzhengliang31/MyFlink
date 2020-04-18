package com.wzl.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

public class DataSetDataSinkAppJ {
    public static void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> list = new ArrayList<Integer>();
        for(int i = 0; i < 10; i++){
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);
        //DataSource<Integer> data = env.fromCollection(list).setParallelism(2);
        String path = "res/sinkdir/test.txt";
        data.writeAsText(path, FileSystem.WriteMode.OVERWRITE);

        env.execute("DataSetDataSinkAppJ");
    }
}
