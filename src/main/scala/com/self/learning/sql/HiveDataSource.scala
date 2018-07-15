package com.self.learning.sql

import com.self.learning.sql.JsonDataSource.conf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveDataSource")
    conf.set("spark.testing.memory", "1073741824")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("drop table if exists student_infos")
    hiveContext.sql("create table if not exists student_infos (name string, age int)")
    hiveContext.sql("load data local inpath '/opt/software/spark-learning/resources/student_infos.txt' into table student_infos")

    hiveContext.sql("drop table if exists student_scores")
    hiveContext.sql("create table if not exists student_scores(name string, score int)")
    hiveContext.sql("load data local inpath '/opt/software/spark-learning/resources/student_scores.txt' into table student_scores")

    val goodStudentInfosDF = hiveContext.sql("select a.name,a.age,b.score from student_infos a " +
      "inner join student_scores b on a.name=b.name where b.score>=80")
    hiveContext.sql("drop table if exists good_student_infos")
    goodStudentInfosDF.write.mode("overwrite").saveAsTable("good_student_infos")

    hiveContext.table("good_student_infos").show()




  }
}
