package com.sh.utils

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DemoTwo {
  def main(args: Array[String]): Unit = {
//        val conf = new SparkConf().setAppName("HiveUtils").setMaster("local[2]")
//        val sc = new SparkContext(conf)
//        val sqlContext = new HiveContext(sc)

    val spark = SparkSession
      .builder()
      .appName("DemoTwo")
      .setMaster("local[2]")
//      .config("hive.metastore.uris", "thrift://localhost:9083") // replace with your hivemetastore service's thrift url
      .config("hive.metastore.uris", "thrift://127.0.0.1:3306")
      .enableHiveSupport() // don't forget to enable hive support
      .getOrCreate()

    val conf =new SparkConf()
    val sc=new SparkContext(conf) // 将配置好的对象作为参数给上下文，上下文作为一个Spark 程序执行的入口
    val sqlContext = new HiveContext(sc)

    sqlContext.sql("show tables")

//    val conf =new SparkConf().setAppName("DemoTwo").setMaster("local[2]")
//    val sc=new SparkContext(conf) // 将配置好的对象作为参数给上下文，上下文作为一个Spark 程序执行的入口
//    val hivecontext= new HiveContext(sc) //再把配置好的上下文对象作为参数传给hive 目的是针对整个APP执行sql

//    hivecontext.sql("userdbname")
//
//    hivecontext.sql("drop table if exists people")
//
//    hivecontext.sql("create table if not exists people(name string,age int) row format delimited fields termimated by '\t' lines terminated by '\n'" )
//
//    hivecontext.sql("load data local inpath 'path'")
//
//    hivecontext.sql("use dbname")
//
//    hivecontext.sql("select * from tablename")

    /*

    * 通过HiveContext使用join直接基于hive中的两种表进行操作

    */

//    val resultDF=hivecontext.sql("select pi.name,pi.age,ps.score "
//
//      +" from people pi join peopleScores ps on pi.name=ps.name"
//
//      +" where ps.score>90")

    /*

    * 通过saveAsTable创建一张hive managed table，数据的元数据和数据即将放的具体位置都是由

    * hive数据仓库进行管理的，当删除该表的时候，数据也会一起被删除（磁盘的数据不再存在）

    */
//
//    hivecontext.sql("drop table if exists peopleResult")
//
//    resultDF.saveAsTable("peopleResult")

    /*

    * 使用HiveContext的table方法可以直接读取hive数据仓库的Table并生成DataFrame,

    * 接下来机器学习、图计算、各种复杂的ETL等操作

    */
//    val dataframeHive = hivecontext.table("hive_bdtable")
//    dataframeHive.show()

  }
}
