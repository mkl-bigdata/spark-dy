package day01

import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

object reduceByKeyDemo {

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


    //使用shuffledRDD来实现reducebykey的相同功能
    val shuffledRDD = new ShuffledRDD[String, Int, Int](

      wordAndOne
      , new HashPartitioner(wordAndOne.partitions.length)

    )

    shuffledRDD.setMapSideCombine(true)

    val f1 = (x :Int) => x
    val f2 = (x:Int,y:Int) => x + y

    val f3 = (x:Int,y:Int) => x + y

    shuffledRDD.setAggregator(new Aggregator[String,Int,Int](

      f1,f2,f3

    ))


    shuffledRDD.saveAsTextFile("shuffled_reducebykey")


    //使用combinebekey来实现reducebykey的相同功能

    val f11 = (x:Int) => x
    val f22 = (x:Int,y:Int) => x+y
    val f33 = (x:Int,y:Int) => x+y

    val combineByKeyRDD: RDD[(String, Int)] = wordAndOne.combineByKey(f11, f22, f33)

    spark.stop()
  }

}
