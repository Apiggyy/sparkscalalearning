package com.self.learning.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object JsonDataSource extends App {
  val conf = new SparkConf().setAppName("JsonDataSource")
  conf.set("spark.testing.memory", "1073741824")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.json("hdfs://spark1:9000/spark-learning/students.json")
  df.registerTempTable("student_scores")
  val studentScoresDF = sqlContext.sql("select name,score from student_scores where score>=80")

  val studentNames = studentScoresDF.rdd.map(row => row.getString(0)).collect()

  val studentInfosJson = Array("{\"name\":\"Leo\",\"age\":18}", "{\"name\":\"Marry\",\"age\":17}", "{\"name\":\"Jack\",\"age\":19}")
  val studentInfosRDD = sc.parallelize(studentInfosJson)
  val stuDf = sqlContext.read.json(studentInfosRDD)
  stuDf.registerTempTable("student_info")
  var sql = "select name,age from student_info where name in ("
  for(i <- 0 until studentNames.length) {
    sql += "'" + studentNames(i) + "'"
    if(i < studentNames.length - 1) {
      sql += ","
    }
  }
  sql += ")"
  val studentInfosDF = sqlContext.sql(sql)
  val goodStudentsRDD = studentScoresDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Long]("score")))
    .join(studentInfosDF.rdd.map(row => (row.getAs[String]("name"), row.getAs[Long]("age"))))

  val goodStudentsRowRDD = goodStudentsRDD.map(info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

  val structType = StructType(
    Array(StructField("name",StringType,true)
         ,StructField("score",IntegerType,true)
         ,StructField("age",IntegerType,true)
    )
  )

  val goodStudentDF = sqlContext.createDataFrame(goodStudentsRowRDD, structType)
  goodStudentDF.write.format("json").save("hdfs://spark1:9000/spark-learning/good_student.json")




}
