package com.self.learning.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)

    val names = Array("Leo", "Marry",  "Jack", "Tom", "Marry", "Tom", "Leo", "Tom")
    val namesRDD = sc.parallelize(names, 5)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.registerTempTable("tbl_names")
    sqlContext.udf.register("strCount", new StringCount)
    val df = sqlContext.sql("select name, strCount(name) from tbl_names group by name")
    df.show()
  }
}
