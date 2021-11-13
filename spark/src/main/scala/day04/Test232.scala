package day04

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Test232 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val inputValue: RDD[String] = spark.sparkContext.textFile("data/exist1")

    println(inputValue.count())

    println(inputValue.distinct().count())


    println("s")



    spark.stop()

  }

}
