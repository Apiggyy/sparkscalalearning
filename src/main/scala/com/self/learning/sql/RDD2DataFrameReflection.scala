package com.self.learning.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameReflection extends App {
  val conf = new SparkConf().setAppName("RDD2DataFrameReflection").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  case class Student(id :Int, name: String, age: Int)

  val rdd = sc.textFile("E:\\迅雷下载\\students.txt")
  val studentsRDD = rdd.map(line => {
    val arr = line.split(",")
    Student(arr(0).trim.toInt, arr(1), arr(2).trim.toInt)
  })
  val studentsDF = studentsRDD.toDF()
  studentsDF.registerTempTable("students")
  val rows = sqlContext.sql("select * from students where age>=18")
//  val students = rows.rdd.map(row => Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt))
//  val students = rows.rdd.map(row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))

  //通过getValuesMap获取几列
  val students = rows.rdd.map(
      row => {
        val map = row.getValuesMap[Any](Array("id", "name", "age"))
        Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
      }
  )
  students.collect().foreach(stu => println(stu.id + "\t" + stu.name + "\t" + stu.age))
}
