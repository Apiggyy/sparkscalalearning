package com.self.learning.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    conf.set("spark.testing.memory","1073741824")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/user/data/test_spark.txt", 1)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreach(wordCount => println(wordCount._1 + " appears " + wordCount._2 + " times."))

  }
}
