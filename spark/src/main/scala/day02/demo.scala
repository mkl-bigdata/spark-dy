package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)

    val value: RDD[Int] = spark.parallelize(List(1, 2, 3, 4, 5))


    var posistion = 1

    val result: RDD[(Int, Int)] = value.map(iter => {

      posistion = posistion + 1

      (posistion,iter)

    })

    val result2: RDD[(Int, Int)] = value.mapPartitionsWithIndex((index, iter) => {

      iter.map(x => {
        (index, x)
      })

    })

    result2.persist()


    println(result2.collect().toBuffer)

    spark.stop()

  }

}
