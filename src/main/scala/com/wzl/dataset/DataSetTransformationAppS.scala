package com.wzl.dataset

import com.wzl.util.DBUtils
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

// 隐式转化
import org.apache.flink.api.scala._

object DataSetTransformationAppS {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    //mapFunction(env)
    //filterFunction(env)
    //mapPartitionFunction(env)
    //firstNFunction(env)
    //flatMapFunction(env)
    //distinctFunction(env)
    //joinFunction(env)
    //outerJoinFunction(env)
    crossFunction(env)
  }

  def mapFunction(env: ExecutionEnvironment): Unit ={
    val data = env.fromCollection(List(1,2,3,4,5,6,7,8,9,0))
    //data.map((x:Int) => x + 1).print()
    //data.map((x) => x + 1).print()
    //data.map(x => x + 1).print()
    data.map(_ + 1).print()
  }

  def filterFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0))
    //data.filter(_ > 5).print()
    data.map(_ + 1).filter(_ > 5).print()
  }

  // 数据量过大性能降低
  def mapPartitionFunction(env: ExecutionEnvironment): Unit ={
    val students = new ListBuffer[String]
    for(i<-1 to 100){
      students.append("student: " + i)
    }

    val data = env.fromCollection(students).setParallelism(4)

//    data.map(x => {
//      val connection = DBUtils.getConnection()
//      println(connection + "......")
//      DBUtils.returnConnection(connection)
//    }).print()

    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(connection + "......")
      DBUtils.returnConnection(connection)
      x
    }).print()
  }

  def firstNFunction(env: ExecutionEnvironment): Unit ={
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((3, "Linux"))
    info.append((4, "Vue"))

    val data = env.fromCollection(info)

    //data.first(3).print()
    //data.groupBy(0).first(2).print()
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
  }

  def flatMapFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[String]()
    info.append("hadoop spark")
    info.append("hadoop flink")
    info.append("flink flink")

    val data = env.fromCollection(info)
    //data.map(_.split(' ')).print()    //数组地址
    //data.flatMap(_.split(' ')).print()
    data.flatMap(_.split(' ')).map((_, 1)).groupBy(0).sum(1).print()
  }

  def distinctFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[String]()
    info.append("hadoop spark")
    info.append("hadoop flink")
    info.append("flink flink")

    val data = env.fromCollection(info)
    data.flatMap(_.split(" ")).distinct().print()
  }

  def joinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "Hadoop"))
    info1.append((2, "Spark"))
    info1.append((3, "Flink"))
    info1.append((4, "Java"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "hdfs"))
    info2.append((2, "rdd"))
    info2.append((3, "blink"))
    info2.append((5, "test"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
  }

  def outerJoinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = ListBuffer[(Int, String)]()
    info1.append((1, "Hadoop"))
    info1.append((2, "Spark"))
    info1.append((3, "Flink"))
    info1.append((4, "Java"))

    val info2 = ListBuffer[(Int, String)]()
    info2.append((1, "hdfs"))
    info2.append((2, "rdd"))
    info2.append((3, "blink"))
    info2.append((5, "test"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
      if(second == null){
        (first._1, first._2, null)
      } else {
        (first._1, first._2, second._2)
      }
    }).print()

//    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
//      if(first == null){
//        (second._1, null, second._2)
//      } else {
//        (second._1, first._2, second._2)
//      }
//    }).print()

//    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
//      if(first == null){
//        (second._1, null, second._2)
//      } else if (second == null) {
//        (first._1, first._2, null)
//      } else {
//        (first._1, first._2, second._2)
//      }
//    }).print()
  }

  // 笛卡尔积
  def crossFunction(env: ExecutionEnvironment): Unit = {
    val info1 = List("Hadoop", "Spark")
    val info2 = List(1, 2, 3)

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.cross(data2).print()
  }
}
