package day01

import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object coGroupDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")
    val words1: RDD[String] = spark.parallelize(

      List("spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka", "hadoop", "flink", "hive", "flink")
      ,4)


    val wordAndOne1: RDD[(String, Int)] = words1.map((_, 1))

    val word2: RDD[String] = spark.parallelize(

      List("spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka", "hadoop", "flink", "hive", "flink")
      , 4)

    val wordAndOne2: RDD[(String, Int)] = word2.map((_, 1))

    println(wordAndOne1.cogroup(wordAndOne2).collect().toBuffer)
  }

}
