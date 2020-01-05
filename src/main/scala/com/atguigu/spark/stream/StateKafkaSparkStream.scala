package com.atguigu.spark.stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateKafkaSparkStream {

  def main(args: Array[String]): Unit = {
    //设置访问hadoop的用户
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StateKafkaSparkStream")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //设置状态保存目录
    streamingContext.checkpoint("hdfs://dsjrz5:9000/wds/streamCheck")

    //2.定义kafka参数
    val brokers = "dsjrz5:9092,dsjrz6:9092,dsjrz7:9092"
    //    val topic = "source"
    val topics = Array("source")
    val consumerGroup = "spark"

    //3.将kafka参数映射为map
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> consumerGroup,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )

    //消费策略
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)

    //4.通过KafkaUtil创建kafkaDSteam
    val sourceDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)

    //values代表key的多个value,optn中存放的是key的旧状态，将新旧状态合并之后就更新状态
    sourceDstream.map(x => (x.value(), 1)).updateStateByKey(
      (values:Seq[Int],optn:Option[Int]) =>{
        val newValue: Int = values.foldLeft(0)(_ + _)
        val oldValue: Int = optn.getOrElse(0)
        Some(newValue + oldValue)
      }
    ).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
