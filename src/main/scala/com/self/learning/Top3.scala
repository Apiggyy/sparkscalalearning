package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = sc.textFile("E:\\迅雷下载\\top.txt")
    val pairs = numbers.map(number => (number.toInt, number))
    val sortedPairs = pairs.sortByKey(false)
    val sortedNumbers = sortedPairs.map(_._2)
    val top3Numers = sortedNumbers.take(3)
    for(num <- top3Numers) {
      println(num)
    }
  }

}
