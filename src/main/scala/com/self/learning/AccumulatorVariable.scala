package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)
    val numbersRDD = sc.parallelize(numbers)
    val sum = sc.longAccumulator("sum")
    numbersRDD.foreach(sum.add(_))
    println("累加值为：" + sum.value)
  }
}
