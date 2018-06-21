package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastVariable").setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3
    val broadcastFactor = sc.broadcast(factor)
    val numbersRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val multiNumbersRDD = numbersRDD.map(_ * broadcastFactor.value)
    multiNumbersRDD.foreach(println(_))

  }

}
