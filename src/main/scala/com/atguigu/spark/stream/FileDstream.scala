package com.atguigu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileDstream {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FileDstream")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))

    val sourceDs: DStream[String] = streamingContext.textFileStream("hdfs://dsjrz5:9000/wds/fileStream")
    val resultTupeDs: DStream[(String, Int)] = sourceDs.flatMap(x => x.split(",")).map((_,1)).reduceByKey(_ + _)
    resultTupeDs.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
