package com.self.learning.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrameOperation {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession
      .builder()
      .appName("DataFrameCreate")
      .config("spark.testing.memory", "1073741824")
      .getOrCreate()
    var df = spark.read.json("hdfs://spark1:9000/user/data/students.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select(df("name"),df("age") + 1).show()
    df.filter(df("age") > 18).show()
    df.groupBy("age").count().show()
  }

}
