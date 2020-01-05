package com.atguigu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val socketDstream: ReceiverInputDStream[String] = ssc.socketTextStream("dsjrz7",9999)
    val ressultDstream: DStream[(String, Int)] = socketDstream.flatMap( x => x.split(",")).map(x => (x,1)).reduceByKey((x,y) => x + y)
    ressultDstream.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
