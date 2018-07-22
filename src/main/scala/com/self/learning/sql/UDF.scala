package com.self.learning.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDF").setMaster("local")
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc)

    val names = Array("Leo", "Marry",  "Jack", "Tom")
    val namesRDD = sc.parallelize(names, 5)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.registerTempTable("tbl_names")
    sqlContext.udf.register("strLen", (str: String) => str.length)
    val df = sqlContext.sql("select name, strLen(name) from tbl_names")
    df.show()

  }

}
