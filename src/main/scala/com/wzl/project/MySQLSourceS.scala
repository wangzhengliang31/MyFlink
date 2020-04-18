package com.wzl.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

class MySQLSourceS extends RichParallelSourceFunction[mutable.HashMap[String, String]] {
  var connect:Connection = null
  var ps:PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.157.157:3306"
    val user = "test"
    val passwd = "test"
    Class.forName(driver)
    connect = DriverManager.getConnection(url, user, passwd)

    val sql = "select id, user_id, domain from user_domain"
    ps = connect.prepareStatement(sql)
  }

  override def close(): Unit = {
    if(connect != null){
      connect.close()
    }

    if(ps != null){
      ps.close()
    }
  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {
    // TODO 从Mysql表中读取出来转成Map进行数据封装
  }

  override def cancel(): Unit = {}
}
