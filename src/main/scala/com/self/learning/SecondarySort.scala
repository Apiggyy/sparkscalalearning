package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object SecondarySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondaySort").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\迅雷下载\\sort.txt", 1)
    val pairs = lines.map(line => (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line))
    val sortedPairs = pairs.sortByKey()
    val sortedLines = sortedPairs.map(_._2)
    sortedLines.foreach(println(_))
  }
}
