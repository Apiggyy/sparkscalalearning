package com.self.learning.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ManullySpecifyOperations extends App {
  val conf = new SparkConf().setAppName("ManullySpecifyOperations").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.format("json").load("E:\\迅雷下载\\people.json")
  df.select("name").write.format("parquet").save("E:\\迅雷下载\\peopleName.parquet")
}
