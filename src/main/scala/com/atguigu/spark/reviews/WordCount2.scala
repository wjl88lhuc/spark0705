package com.atguigu.spark.reviews

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val sparkWdConf: SparkConf = new SparkConf().setAppName("sparkWd")
    val context = new SparkContext(sparkWdConf)

    val lines: RDD[String] = context.textFile(args(0))
//    lines.flatMap(x =>x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => x + y)
    lines.flatMap(x =>x.split(" ")).map(x => (x,1)).reduceByKey{
      case (x,y) =>{
        x + y
      }
    }
      .saveAsTextFile(args(1))
//    val rdd1: RDD[(String, Int)] = context.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
//    rdd1.aggregateByKey()
    context.stop()
  }

}
