package com.sh.utils

import org.apache.spark.sql.SparkSession

object ScalaConnection {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("ScalaConnection")
        .config("hive.metastore.uris", "thrift://{server ip}:9083") //
        .enableHiveSupport()
        .getOrCreate()

      import spark.sql
      sql("show tables").show()
      sql("SELECT * FROM hive_table").show()
      spark.stop()
    }

}
