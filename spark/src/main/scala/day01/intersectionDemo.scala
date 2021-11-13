package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object intersectionDemo {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val rdd1: RDD[Int] = spark.parallelize(List(1, 2, 3, 4), 2)


    val rdd2: RDD[Int] = spark.parallelize(List(5, 2, 3, 9), 2)


   // println(rdd1.intersection(rdd2).collect().toBuffer)

    //使用cogroup来实现intersection
    val rdd_1: RDD[(Int, Null)] = rdd1.map((_, null))

    val rdd_2: RDD[(Int, Null)] = rdd2.map((_, null))

    val result: RDD[Int] = rdd_1.cogroup(rdd_2)
      .filter { case (_, (iter1, iter2)) => iter1.nonEmpty && iter2.nonEmpty }
      .keys


    println(result.collect().toBuffer)

  }

}
