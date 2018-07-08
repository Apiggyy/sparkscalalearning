package com.self.learning.sql

import com.self.learning.sql.RDD2DataFrameReflection.{sc, sqlContext}
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, RowFactory, SQLContext, types}

import scala.collection.mutable.ArrayBuffer

object RDD2DataFrameProgrammaticlly extends App {
  val conf = new SparkConf().setAppName("RDD2DataFrameProgrammaticlly").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val lines = sc.textFile("E:\\迅雷下载\\students.txt")
  val studentsRDD = lines.map(line => {
    val splitedLine = line.split(",")
    Row(splitedLine(0).toInt, splitedLine(1), splitedLine(2).toInt)
  })
  val structType = types.StructType(Array(
          StructField("id", IntegerType)
         ,StructField("name", StringType)
         ,StructField("age", IntegerType)
      )
  )
  val df = sqlContext.createDataFrame(studentsRDD, structType)
  df.registerTempTable("students")
  val studentsDF = sqlContext.sql("select * from students where age>=18")
  studentsDF.rdd.collect().foreach(println(_))

}
