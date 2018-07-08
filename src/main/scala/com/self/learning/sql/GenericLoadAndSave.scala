package com.self.learning.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object GenericLoadAndSave extends App {
  val conf = new SparkConf().setAppName("GenericLoadAndSave").setMaster("local")
  val sc = new SparkContext(conf);
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.load("E:\\迅雷下载\\users.parquet")
  df.select("name", "favorite_color").write.save("E:\\迅雷下载\\NameAndFavoriteColor.parquet")
}
