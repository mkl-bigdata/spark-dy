package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object joinDemo {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val words1: RDD[(String, Int)] = spark.parallelize(

      List(("spark", 1), ("hadoop", 2), ("hive", 3), ("spark", 4)), 2)


    val words2: RDD[(String, Int)] = spark.parallelize(

      List(("hive", 1), ("spark", 2), ("hadoop", 3)), 2

    )


    println(words1.join(words2).collect().toBuffer)


    //使用cogroup来实现join
    val result: RDD[(String, (Int, Int))] = words1.cogroup(words2)
      .flatMapValues { case (iter1, iter2) => {

        for (x <- iter1.iterator; y <- iter2.iterator) yield (x, y)

      }
      }

    println(result.collect().toBuffer)

  }

}
