package com.self.learning.core

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {

  def main(args: Array[String]): Unit = {
//    map()
//    filter()
//    flagMap()
//    groupByKey()
//    reduceByKey()
//    sortByKey()
//    join()
    cogroup()
  }

  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)

    val numberRDD = sc.parallelize(Array(1, 2, 3, 4, 5), 1)
    val newNumberRDD = numberRDD.map(_ * 2)
    newNumberRDD.foreach(println(_))
  }

  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)

    val numberRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1)
    val newNumberRDD = numberRDD.filter(_ % 2 == 0)
    newNumberRDD.foreach(println(_))
  }

  def flagMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc = new SparkContext(conf)

    val numberRDD = sc.parallelize(Array("hello you", "hello me", "hello you", "hello everyone"),1)
    val newNumberRDD = numberRDD.flatMap(_.split(" "))
    newNumberRDD.foreach(println(_))
  }

  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val socreList = Array(("class1", 90), ("class2", 80), ("class1", 70), ("class2", 90))
    val scorePairs  = sc.parallelize(socreList, 1)
    val scoreGroup = scorePairs.groupByKey()
    scoreGroup.foreach(t => {
      println("class: " + t._1)
      t._2.foreach(println(_))
      println("=============================")
    })
  }

  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = sc.parallelize(Array(("class1", 90), ("class2", 80), ("class1", 70), ("class2", 60)), 1)
    val scores = scoreList.reduceByKey(_ + _)
    scores.foreach(t => {
      println("class: " + t._1)
      println("class: " + t._2)
      println("==================================")
    })
  }

  def sortByKey(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = sc.parallelize(Array((80, "Alice"), (90, "Bob"), (100, "Cindy"), (70, "Jack")),1)
    val scoreSorted = scoreList.sortByKey(false)
    scoreSorted.foreach(t => println(t._1 + " : " + t._2))
  }

  def join(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val students = Array((1, "Alice"), (2, "Bob"), (3, "Cindy"))
    val scores = Array((1, 100), (2, 90), (3, 80))

    val studnetsRDD = sc.parallelize(students)
    val scoresRDD = sc.parallelize(scores)

    val joinRDD = studnetsRDD.join(scoresRDD)
    joinRDD.foreach(t => {
      println("id : " + t._1)
      println("name : " + t._2._1)
      println("score : " + t._2._2)
      println("===============================")
    })
  }

  def cogroup(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local")
    val sc = new SparkContext(conf)

    val students = Array((1, "Alice"), (2, "Bob"), (3, "Cindy"))
    val scores = Array((1, 100), (2, 90), (3, 80), (1, 99), (3, 85))

    val studentsRDD = sc.parallelize(students)
    val scoresRDD = sc.parallelize(scores)

    val cogroupRDD = studentsRDD.cogroup(scoresRDD)
    cogroupRDD.foreach(t => {
      println("id : " + t._1)
      println("name : " + t._2._1)
      println("score : " + t._2._2)
      println("===================================")
    })
  }

}
