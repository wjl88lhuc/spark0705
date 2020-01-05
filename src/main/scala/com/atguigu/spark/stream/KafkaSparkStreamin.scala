package com.atguigu.spark.stream

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkStreamin {
  def main(args: Array[String]): Unit = {

    //1.创建SparkConf并初始化SSC
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreamin")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

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
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true:java.lang.Boolean),
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers
    )

    //消费策略
    val consumerStrategy = ConsumerStrategies.Subscribe[String,String](topics, kafkaParams)

    //4.通过KafkaUtil创建kafkaDSteam
    val sourceDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String,String](streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)

    val result: DStream[(String, String)] = sourceDstream.map(x =>(x.key(),x.value()))
    result.map(rdd =>{
      (rdd._2,1)
    }).reduceByKey(_ + _).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
