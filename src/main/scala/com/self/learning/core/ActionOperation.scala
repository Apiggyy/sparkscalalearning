package com.self.learning.core

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {

  def main(args: Array[String]): Unit = {
    //    reduce()
    //    collect()
    //    count()
    //    take()
//    saveAsTextFile()
    countByKey()
  }

  def reduce(): Unit = {
    val conf = new SparkConf().setAppName("reduce").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val sum = numbers.reduce(_ + _)
    println("总和为： " + sum)
  }

  def collect(): Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val numbersRDD = numbers.map(_ * 2)
    val multiNumbers = numbersRDD.collect();
    for (number <- multiNumbers) {
      println(number)
    }
  }

  def count(): Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val count = numbers.count()
    println(count)
  }

  def take(): Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val top3Number = numbers.take(3)
    for (number <- top3Number) {
      println(number)
    }
  }

  def saveAsTextFile(): Unit = {
    val conf = new SparkConf().setAppName("saveAsTextFile")
    conf.set("spark.testing.memory", "1073741824")
    val sc = new SparkContext(conf)

    val numbers = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val doubleNumbers = numbers.map(_ * 2)
    doubleNumbers.saveAsTextFile("hdfs://spark1:9000/user/data/doubleNumber_scala.txt")
  }

  def countByKey(): Unit = {
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val studentsList = Array(("class1", "Alice"), ("class2", "Bob"), ("class1", "Cindy"), ("class2", "David"), ("class1", "Jack"))
    val studentsRDD = sc.parallelize(studentsList)
    val result = studentsRDD.countByKey()
    for((key, value) <- result) {
      println(key + " : " + value)
    }
  }

}
