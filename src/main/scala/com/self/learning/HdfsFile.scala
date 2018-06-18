package com.self.learning

import org.apache.spark.{SparkConf, SparkContext}

object HdfsFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hdfsfile");
    conf.set("spark.testing.memory","1073741824")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("hdfs://spark1:9000/user/data/test_spark.txt", 5)
    val lineLength = lines.map(_.length)
    val count = lineLength.reduce(_ + _)
    println("文件总字数为：" + count)
  }
}
