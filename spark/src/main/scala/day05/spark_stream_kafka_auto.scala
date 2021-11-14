package day05

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object spark_stream_kafka_auto {


  def main(args: Array[String]): Unit = {

     val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)


    val spark = new StreamingContext(conf, Seconds(5))

    spark.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String,Object](


      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "g02",
    "auto.offset.reset" -> "earliest"

    )

    val topic = Array("wordcount")

    val inputStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      spark,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    )


    val result: DStream[(String, Int)] = inputStream.map(x => x.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()

    spark.start()

    spark.awaitTermination()

  }

}
