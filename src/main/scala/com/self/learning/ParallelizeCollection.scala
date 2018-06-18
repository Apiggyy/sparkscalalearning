package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numRdd = sc.parallelize(numbers, 1)
    val sum  = numRdd.reduce(_ + _)
    println("sum : " + sum)
  }

}
