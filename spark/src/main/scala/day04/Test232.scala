package day04

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.LongAccumulator

object Test232 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()



    val scores: RDD[(String, Double)] =spark.sparkContext.parallelize(List(("chinese", 88.0) , ("chinese", 90.5) , ("math", 60.0), ("math", 87.0)))


    val result: RDD[(String, (Double, Int))] = scores.combineByKey(
      v => (v, 1),
      (acc: (Double, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Double, Int), acc2: (Double, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

    )

    spark.stop()

  }

}
