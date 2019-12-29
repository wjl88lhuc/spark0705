package com.atguigu.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    println("===========111===========")
//    val sparkConf = new SparkConf().setAppName("WordCount")
    System.setProperty("HADOOP_USER_NAME","hadoop")
    println("===========222===========")
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)
    println("===========333===========")
    val file = sparkContext.textFile("hdfs://dsjrz2:9000/RELEASE")
    println("===========444===========")
    val words = file.flatMap(_.split(" "))
    println("===========555===========")
    val wordTuple = words.map((_,1))
    println("===========666===========")
    wordTuple.reduceByKey(_ + _).saveAsTextFile("hdfs://dsjrz2:9000/spark-out1")
    println("===========777===========")
    sparkContext.stop()
  }
}
