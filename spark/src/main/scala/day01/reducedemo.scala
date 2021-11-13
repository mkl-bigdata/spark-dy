package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object reducedemo {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")


    val words: RDD[Int] = spark.parallelize(

      List(1,2,3,4,5,6,7,8,9)
      ,4)

    val result: Int = words.reduce(_ + _)

    println(result)

    Thread.sleep(10000000)
  }
}
