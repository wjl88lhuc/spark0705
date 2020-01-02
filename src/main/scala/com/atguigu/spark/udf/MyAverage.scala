package com.atguigu.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object MyAverage {
  def main(args: Array[String]): Unit = {
    //本地运行是一定要把pom文件中的 <scope>provided</scope>注销掉
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql-demo1")
    //sparkSession 是sparkSql的入口
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //创建聚合函数对象
    val myAvrg = new MyAverageFunction
    //将聚合函数注册到 SparkSession中
    sparkSession.udf.register("avrSalary", myAvrg) //到此位置，我们就成功注册了一个名为 avrSalary 的函数,这个函数可以像 max(),min(),count()那样的函数使用

    // DateSet,DateFrame,Rdd之间的相互转换需要引入隐式转换  import sparkSession.implicits._
    import sparkSession.implicits._
    //从hdfs中读取数据，并转换成DateFrame
    //    val df1: DataFrame = sparkSession.sparkContext.textFile("hdfs://dsjrz5:9000/wds/sql/txt/input/peopel.txt").map(x => {  //从hdfs上读取的
    val df1: DataFrame = sparkSession.sparkContext.textFile("in/people.txt").map(x => {  //从本地读取的
      val fields: Array[String] = x.split(",")
      People(fields(0).trim.toDouble, fields(1).trim.toLong, fields(2).trim, fields(3).trim, fields(4).trim.toDouble, fields(5).trim.toLong)
    }).toDF()
    //创建临时表
    df1.createOrReplaceTempView("people")

    sparkSession.sql("select avrSalary(salary) from people").show()

    //释放关闭资源
    sparkSession.stop()
  }
}

case class People(comm: Double, id: Long, profession: String, name: String, salary: Double, age: Long)

/**
  * 需求：求员工的平均工资
  * 思路：
  * （1）输入各个员工的工资
  * （2）在缓冲区中统计员工的个数，统计员工的总工资
  * （3）将员工的总工资除以员工个数，得到员工的平均工资
  * （4）将员工的平均工资输出
  */
class MyAverageFunction extends UserDefinedAggregateFunction {
  //函数输入的数据结构
  //  override def inputSchema: StructType = StructType(StructField("inputColumn",LongType) :: Nil)
  override def inputSchema: StructType = {
    new StructType().add("caonima", LongType)  //  caonima这个名字随便写，最好还是见名知意
  }

  //计算时的数据结构(也就是缓冲区的数据结构)
  //  override def bufferSchema: StructType = StructType(StructField("sum",LongType) :: StructField("count",LongType) :: Nil)
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数计算完成之后，返回的数据类型
  override def dataType: DataType = DoubleType

  //表示这个自定义的函数是否稳定：也就是说当输入值相同的时候，这个函数计算的结果是否是一样的
  override def deterministic: Boolean = true

  //计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 存工资的总额
    buffer(0) = 0L
    // 存工资的个数
    buffer(1) = 0L
  }

  //缓冲区的更新（缓冲区中运用的变量是保存在buffer 数组中，顺序更添加的顺序保持一致，所以通过下标index可以取出）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //多个分区的合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //函数计算操作的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
