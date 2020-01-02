package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql01_demo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSql01_demo")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    spark.stop()
  }

}
