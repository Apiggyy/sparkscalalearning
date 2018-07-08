package com.self.learning.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object ParquetMergeSchema extends App {
  val conf = new SparkConf().setAppName("ParquetMergeSchema")
  conf.set("spark.testing.memory","1073741824")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val studentsWithNameAndAge = Array(("leo", 21),("jack", 22))
  val studentsWithNameAndAgeDf = sc.parallelize(studentsWithNameAndAge, 2).toDF("name", "age")
  studentsWithNameAndAgeDf.write.mode("append").parquet("hdfs://spark1:9000/spark-learning/students")

  val studentsWithNameAndGrade = Array(("marry", "A"),("tom", "B"))
  val studentsWithNameAndGradeDf = sc.parallelize(studentsWithNameAndGrade, 2).toDF("name", "grade")
  studentsWithNameAndGradeDf.write.mode("append").parquet("hdfs://spark1:9000/spark-learning/students")

  val mergeDF = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://spark1:9000/spark-learning/students")
  mergeDF.printSchema()
  mergeDF.show();


}
