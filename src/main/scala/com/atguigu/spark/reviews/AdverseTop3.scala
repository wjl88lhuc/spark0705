package com.atguigu.spark.reviews

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AdverseTop3 {
  /**
    * 需求：统计出每一个省份广告被点击次数的TOP3
    *
    * 数据在  adData.txt文件中，字段之间的分隔符是空格
    * 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val adTopConf: SparkConf = new SparkConf().setAppName("adTop3")
    val context = new SparkContext(adTopConf)
    val hadoopRDD: RDD[String] = context.textFile("hdfs://dsjrz5:9000/wds/ads/input/advertise.txt")
    val cityAdRDD: RDD[(String, Int)] = hadoopRDD.map(x => {
      val fields: Array[String] = x.split(" ")
      val newKey: String = fields(1) + "-" + fields(4)
      (newKey, 1)
    }).reduceByKey((x, y) => x + y)
    val cityKeyRDD: RDD[(String, (String, Int))] = cityAdRDD.map(func3)
    val cityGrpRDD: RDD[(String, Iterable[(String, Int)])] = cityKeyRDD.groupByKey()
    val cityAdTop3: RDD[(String, List[(String, Int)])] =
      cityGrpRDD.mapValues(x => x.toList.sortWith((x,y) =>x._2 > y._2 ).take(3))
    cityAdTop3.saveAsTextFile(args(0))
    context.stop()
  }

  def func3(keyValue:(String,Int)) :(String,(String,Int))={
    val wds = keyValue._1.split("-")
    val newKey = wds(0)
    val newValue = (wds(1), keyValue._2)
    (newKey,newValue)
  }



}
