package com.atguigu.spark.stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object KafkaWindowDstream {
  def main(args: Array[String]): Unit = {
    //设置访问hadoop的用户，这个要设置在 streamingContext创建的前面，要不然就报错了
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaWindowDstream")
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

//    sourceDstream.map(x => (x.value(),1)).reduceByKeyAndWindow((x:Int,y:Int) => x + y,Seconds(20),Seconds(5)).print()
    val wordToCountArrayDstream: DStream[Array[(String, Int)]] = sourceDstream.map {
      rdd => {
        rdd.value().split(" ").map((_, 1))
      }
    }
    val wordToCountDstream: DStream[(String, Int)] = wordToCountArrayDstream.flatMap(x => x)
    val wordToCountWindowDstream: DStream[(String, Int)] = wordToCountDstream.reduceByKeyAndWindow((x:Int,y:Int) =>x + y,Minutes(2),Seconds(5))
    wordToCountWindowDstream.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
