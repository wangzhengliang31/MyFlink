package com.wzl.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * flink 1.7
 */

public class TableSQLAPIJ {
    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
//
//        String path = "res/person.csv";
//        DataSet<Person> csv = env.readCsvFile(path).ignoreFirstLine().pojoType(Person.class, "name", "age", "job", "salary");
//
//        Table personTable = tableEnv.fromDataSet(csv);
//        tableEnv.registerTable("person", personTable);
//        Table resultTable = tableEnv.sqlQuery("select name, age * salary from person");
//
//        tableEnv.toDataSet(resultTable, Row.class).print();
    }

    public static class Person{
        public String name;
        public int age;
        public String job;
        public int salary;
    }
}
