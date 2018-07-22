package com.self.learning.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class StringCount extends UserDefinedAggregateFunction {

  //指的是输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str", StringType)))
  }

  //指的是中间进行聚合时，所处理的数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType)))
  }

  // 指的是函数返回值类型
  override def dataType: DataType = {
    IntegerType
  }

  override def deterministic: Boolean = true

  //为每个分组的数据进行初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //指的是，每个分组有新的值进来的时候，如果进行分组对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //由于Spark是分布式的，所以一个分组的数据，可能会在不同的节点上进行局部聚合，就是update
  //但是最后一个分组，在各个节点上进行聚合，要进行merge，也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)  + buffer2.getAs[Int](0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
