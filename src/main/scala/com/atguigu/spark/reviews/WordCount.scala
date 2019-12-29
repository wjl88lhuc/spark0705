package com.atguigu.spark.reviews

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hadoop")
    val sparkConf = new SparkConf().setAppName("wordCount")
    val sparkContext = new SparkContext(sparkConf)
    val file = sparkContext.textFile(args(0))
    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    sparkContext.stop()
  }

}
