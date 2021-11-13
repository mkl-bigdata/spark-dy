package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object leftOutJoinDemo {



  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)
    spark.setLogLevel("warn")

    val words1: RDD[(String, Int)] = spark.parallelize(

      List(("spark", 1), ("hadoop", 2), ("hive", 3), ("flink", 4)), 2)


    val words2: RDD[(String, Int)] = spark.parallelize(

      List(("hive", 1), ("spark", 2), ("hadoop", 3)), 2

    )


    val leftOutJoinRDD: RDD[(String, (Int, Option[Int]))] = words1.cogroup(words2)
      .flatMapValues { case (iter1, iter2) =>

        if (iter2.iterator.isEmpty) {
          iter1.map(x => (x, None))
        } else {
          for (x <- iter1; y <- iter2) yield (x, Some(y))
        }

      }


    val rightOutJoinRDD: RDD[(String, (Option[Int], Int))] = words1.cogroup(words2)
      .flatMapValues { case (iter1, iter2) =>

        if (iter1.iterator.isEmpty) {
          iter2.map(x => (None, x))
        } else {
          for (x <- iter1; y <- iter2) yield (Some(x), y)
        }

      }


    val fullOutJoinRDD: RDD[(String, (Option[Int], Option[Int]))] = words1.cogroup(words2)
      .flatMapValues {

        case (iter1, Seq()) => iter1.iterator.map(x => (Some(x),None))
        case (Seq(), iter2) => iter2.iterator.map(x => (None,Some(x)))
        case (iter1,iter2) => for(x <- iter1 ; y <- iter2) yield (Some(x),Some(y))

      }

    println(leftOutJoinRDD.collect().toBuffer)

    println(rightOutJoinRDD.collect().toBuffer)

    println(fullOutJoinRDD.collect().toBuffer)



  }

}
