package day01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object foldByKeyDemo {


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


    //foldByKey方法
    val foldByKeyRDD: RDD[(String, Int)] = wordAndOne.foldByKey(0)(_ + _)

    foldByKeyRDD.saveAsTextFile("foldByKey")

    spark.stop()
  }

}
