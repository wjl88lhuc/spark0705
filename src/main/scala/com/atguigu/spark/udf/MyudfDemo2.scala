package com.atguigu.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object MyudfDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyudfDemo2")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val dataset: Dataset[Worker] = sparkSession.sparkContext.textFile("in/workers.txt").map(x => {
      val fields: Array[String] = x.split(",")
      Worker(fields(0).trim, fields(1).trim, fields(2).trim.toLong)
    }).toDS()
    val average_salary: TypedColumn[Worker, Double] = new MyAverage2().toColumn.name("average_salary")
    dataset.select(average_salary).show()
    sparkSession.stop()
  }
}

case class Worker(name: String, occupation: String, age: Long)

case class Average(var sum: Long, var count: Long)

/**
  * 需求： 统计员工平均年龄
  * 思路：
  * （1）函数结构员工 的年龄，即输入
  * （2）在缓冲区中统计员工的总年龄，总人数
  * （3）把缓冲区的总年龄与总人数运算就得到了平均年龄
  * （4）把平均年龄返回，就是输出
  */
class MyAverage2 extends Aggregator[Worker, Average, Double] {
  //初始化缓冲区的数据
  override def zero: Average = Average(0L, 0L)

  //更新缓冲区数据
  override def reduce(b: Average, a: Worker): Average = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  //不同分区的合并（最终的合并，就是driver把不同分区中的进行合并）
  override def merge(b1: Average, b2: Average): Average = {
    b1.sum = b1.sum + b2.sum
    b1.count =b1.count + b2.count
    b1
  }

  //最终的计算结果，返回输出
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble / reduction.count
  }

  // Encoders.product是进行scala元组和case类转换的编码器
  override def bufferEncoder: Encoder[Average] = Encoders.product

  // 设定最终输出值的编码器
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
