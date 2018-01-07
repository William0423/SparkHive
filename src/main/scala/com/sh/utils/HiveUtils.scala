package com.sh.utils

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object HiveUtils {
  def main(args: Array[String]): Unit = {
//    val tablename = "hive_bdtable";
//
//    val HIVE_SERVER = "jdbc:hive2://localhost:10000/default"
//    Class forName("org.apche.hive.jdbc.hiveDriver")
//    val conn=DriverManager.getConnection(HIVE_SERVER,"hadoop", "hive")
//    val stmt =conn.createStatement()
//    val addSQL="select * from " + tablename
//    val res = stmt.execute(addSQL)
//    conn.close()



    println("*****************")
    val conf = new SparkConf().setAppName("HiveUtils").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val hivecontext= new HiveContext(sc) //再把配置好的上下文对象作为参数传给hive 目的是针对整个APP执行sql
    val dataframeHive = hivecontext.table("hive_bdtable")
    dataframeHive.show()

//    sqlContext.table("hive.hive_bdtable") // 库名.表名 的格式
//      .registerTempTable("hive_bdtable")  // 注册成临时表
//    sqlContext.sql(
//      """
//        | select *
//        |   from hive_bdtable
//        |  limit 10
//      """.stripMargin).show()

    sc.stop()
  }

//  def dropPartitions(tablename:String,hivecotext:HiveContext):Unit = {
//
//    sqlContext.sql()
//
//  }
}
