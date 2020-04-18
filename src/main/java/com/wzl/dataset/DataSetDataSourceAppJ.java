package com.wzl.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataSetDataSourceAppJ {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //fromCollect(env);
        //fromTextFile(env);
        fromTextDirectory(env);
    }

    // 从集合中读取
    public static void fromCollect(ExecutionEnvironment env) throws Exception{
        List<Integer> list = new ArrayList<Integer>();
        for (int i=0; i<=10; i++){
            list.add(i);
        }
        env.fromCollection(list).print();
    }

    // 从文件中读取
    public static void fromTextFile(ExecutionEnvironment env) throws Exception{
        String path = "res/WC.txt";
        env.readTextFile(path).print();
    }

    // 从文件夹中读取
    public static void fromTextDirectory(ExecutionEnvironment env) throws Exception{
        String path = "res/";
        env.readTextFile(path).print();
    }
}

