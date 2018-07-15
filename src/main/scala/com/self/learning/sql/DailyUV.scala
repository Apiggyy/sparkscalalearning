package com.self.learning.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DailyUV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DailyUV")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val userAccessLog = Array(
      "2018-06-18,1122",
      "2018-06-18,1122",
      "2018-06-18,1123",
      "2018-06-18,1124",
      "2018-06-18,1124",
      "2018-06-19,1122",
      "2018-06-19,1121",
      "2018-06-19,1123",
      "2018-06-19,1123"
    )

    val userAccessLogRDD = spark.sparkContext.parallelize(userAccessLog, 5)
    val userAccessLogRowRDD = userAccessLogRDD.map(log => Row(log.split(",")(0), log.split(",")(1).toInt))
    val structType = StructType(Array(StructField("date", StringType, true),StructField("userid", IntegerType, true)))

    val userAccessLogDF = spark.sqlContext.createDataFrame(userAccessLogRowRDD, structType)
    userAccessLogDF.groupBy("date")
      .agg(countDistinct("userid"))
      .show()
  }
}
