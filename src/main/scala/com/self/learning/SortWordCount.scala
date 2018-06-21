package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("sortWordCount").setMaster("local")
    var sc = new SparkContext(conf)

    val lines = sc.textFile("E:\\迅雷下载\\test_spark.txt")
    val linesRDD = lines.flatMap(_.split(" "))
    val pairs = linesRDD.map((_, 1))
    val wordsCount = pairs.reduceByKey(_ + _)
    val countWords = wordsCount.map(word => (word._2, word._1))
    val sortedCountWords = countWords.sortByKey(ascending = false)
    val sortedWordsCount = sortedCountWords.map(word => (word._2, word._1))
    sortedWordsCount.foreach(t => println(t._1 + " appears " + t._2 + " times."))

  }
}
