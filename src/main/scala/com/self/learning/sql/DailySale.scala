package com.self.learning.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object DailySale {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DailyUV")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val userSaleLog = Array(
      "2018-06-18, 55.05",
      "2018-06-18, 45.02",
      "2018-06-19, 100.05",
      "2018-06-19, 22.05",
      "2018-06-20, 49.05",
      "2018-06-19, 87.05",
      "2018-06-20, 21.45"
    )
    val userAccessLogRDD = spark.sparkContext.parallelize(userSaleLog, 5)
    val userAccessLogRowRDD = userAccessLogRDD.map(log => Row(log.split(",")(0), log.split(",")(1).toDouble))
    val structType = StructType(Array(StructField("date", StringType), StructField("sale" , DoubleType)))
    val userAccessLogDF = spark.sqlContext.createDataFrame(userAccessLogRowRDD, structType)
    userAccessLogDF.groupBy("date").agg(sum("sale")).orderBy("date").show()

  }
}
