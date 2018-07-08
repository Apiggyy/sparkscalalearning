package com.self.learning.sql

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession
      .builder()
      .appName("DataFrameCreate")
      .config("spark.testing.memory", "1073741824")
      .getOrCreate()
    var df = spark.read.json("hdfs://spark1:9000/user/data/students.json")
    df.show()

  }

}
