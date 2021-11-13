package day01

import java.util

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object groupbykeyDemo {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("groupbykeyDemo")
      .setMaster("local[2]")


    val spark = new SparkContext(conf)


    val words: RDD[String] = spark.parallelize(

      List("spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka", "hadoop", "flink", "hive", "flink")
    ,4)


    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

   /* val result: RDD[(String, Iterable[Int])] = wordAndOne.groupByKey()

    result.saveAsTextFile("group-out")*/


    //使用shuffledRDD来实现groupbykey的方法
    // ShuffledRDD的参数 [key，value，聚合以后的数据格式]
    val shuffledRDD = new ShuffledRDD[String, Int, ArrayBuffer[Int]](

      wordAndOne,
      new HashPartitioner(wordAndOne.partitions.length)
    )

    shuffledRDD.setMapSideCombine(false)

    val innerpartition1 = (x:Int) => ArrayBuffer(x)
    val innerpartition2 = (ab:ArrayBuffer[Int],ele:Int) => ab += ele


    val eachpartition = (ab1:ArrayBuffer[Int],ab2:ArrayBuffer[Int]) => ab1 ++= ab2

    shuffledRDD.setAggregator(new Aggregator[String,Int,ArrayBuffer[Int]](

      innerpartition1,
      innerpartition2,
      eachpartition

    ))


    shuffledRDD.saveAsTextFile("shuffled-out")
    spark.stop()
  }

}
