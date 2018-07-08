package com.self.learning.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ParquetLoadData extends App {
  val conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.parquet("E:\\迅雷下载\\users.parquet")
  df.registerTempTable("users")
  val userNamesDF = sqlContext.sql("select name from users")
  userNamesDF.rdd.map("name : " + _(0)).collect().foreach(println(_))

}
