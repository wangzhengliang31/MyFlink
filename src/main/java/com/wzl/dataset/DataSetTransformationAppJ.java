package com.wzl.dataset;

import com.wzl.util.DBUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class DataSetTransformationAppJ {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // mapFunction(env);
        //filterFunction(env);
        //mapPartitionFunction(env);
        //firstNFunction(env);
        //flatMapFunction(env);
        //distinctFunction(env);
        //joinFunction(env);
        //outerJoinFunction(env);
        crossFunction(env);
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for(int i=0; i<10; i++){
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        }).print();
    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for(int i=0; i<10; i++){
            list.add(i);
        }

        DataSource<Integer> data = env.fromCollection(list);
        data.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer > 5;
            }
        }).print();
    }

    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        for(int i=0; i<100; i++){
            list.add("student: " +i);
        }

        DataSource<String> data = env.fromCollection(list).setParallelism(6);
//        data.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String input) throws Exception {
//                String connection = DBUtils.getConnection();
//                System.out.println("connection...... " + connection);
//                DBUtils.returnConnection(connection);
//                return input;
//            }
//        }).print();
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> input, Collector<String> collector) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println("connection...... " + connection);
                DBUtils.returnConnection(connection);
            }
        }).print();
    }

    public static void firstNFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2(1, "Hadoop"));
        info.add(new Tuple2(1, "Spark"));
        info.add(new Tuple2(1, "Flink"));
        info.add(new Tuple2(2, "Java"));
        info.add(new Tuple2(3, "Linux"));
        info.add(new Tuple2(4, "Vue"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);
        //data.first(2).print();
        //data.groupBy(0).first(2).print();
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop spark");
        info.add("hadoop flink");
        info.add("flink flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(" ");
                for(String split : splits){
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).groupBy(0).sum(1).print();

    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop spark");
        info.add("hadoop flink");
        info.add("flink flink");

        DataSource<String> data = env.fromCollection(info);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] splits = s.split(" ");
                for(String split : splits){
                    collector.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "Hadoop"));
        info1.add(new Tuple2(2, "Spark"));
        info1.add(new Tuple2(3, "Flink"));
        info1.add(new Tuple2(4, "Java"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "hdfs"));
        info2.add(new Tuple2(2, "rdd"));
        info2.add(new Tuple2(3, "blink"));
        info2.add(new Tuple2(5, "test"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
            }
        }).print();
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "Hadoop"));
        info1.add(new Tuple2(2, "Spark"));
        info1.add(new Tuple2(3, "Flink"));
        info1.add(new Tuple2(4, "Java"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "hdfs"));
        info2.add(new Tuple2(2, "rdd"));
        info2.add(new Tuple2(3, "blink"));
        info2.add(new Tuple2(5, "test"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

//        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//            @Override
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(second == null){
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, null);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();

//        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//            @Override
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first == null){
//                    return new Tuple3<Integer, String, String>(second.f0, null, second.f1);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();

        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if(first == null){
                    return new Tuple3<Integer, String, String>(second.f0, null, second.f1);
                } else if(second == null){
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, null);
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<String>();
        info1.add("Hadoop");
        info1.add("Spark");

        List<Integer> info2 = new ArrayList<Integer>();
        info2.add(1);
        info2.add(2);
        info2.add(3);

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }
}
