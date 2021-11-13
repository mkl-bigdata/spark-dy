package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateByKeyDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")


    val words: RDD[String] = spark.parallelize(

      List("spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka", "hadoop", "flink", "hive", "flink")
      ,4)


    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))


    val result: RDD[(String, Int)] = wordAndOne.aggregateByKey(100)(_ + _, _ + _)

    result.saveAsTextFile("aggregateByKey")

    spark.stop()
  }

}
