package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\迅雷下载\\test_spark_01.txt", 1)
    val pairs = lines.map((_, 1))
    val lineCount = pairs.reduceByKey(_ + _)
    lineCount.foreach(line => println(line._1 + " appears " + line._2 + " times."))
  }
}
