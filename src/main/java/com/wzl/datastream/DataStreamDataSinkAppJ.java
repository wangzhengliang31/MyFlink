package com.wzl.datastream;

import com.wzl.bean.Student;
import com.wzl.util.SinkToMySQLJ;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义sink
 * 实现RichSinkFunction<T>
 * 重写方法
 *      open    生命周期方法
 *      invoke  每条记录执行一次
 */
public class DataStreamDataSinkAppJ {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Student> student = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                String[] splits = s.split(" ");
                Student stu = new Student();
                stu.setId(Integer.parseInt(splits[0]));
                stu.setName(splits[1]);
                stu.setAge(Integer.parseInt(splits[2]));

                return stu;
            }
        });

        student.addSink(new SinkToMySQLJ()).setParallelism(1);

        env.execute("DataStreamDataSinkAppJ");
    }
}
