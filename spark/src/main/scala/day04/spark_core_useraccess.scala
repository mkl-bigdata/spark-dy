package day04

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_core_useraccess {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)


    val spark = new SparkContext(conf)


    val inputRDD: RDD[String] = spark.textFile("data/data1.txt")


    val mappedRDD: RDD[(String, String)] = inputRDD.map(x => {

      val fields: Array[String] = x.split(" ")

      (fields(0), fields(1))

    })

    val groupedRDD: RDD[(String, Iterable[String])] = mappedRDD.groupByKey()

    val flatmappedRDD: RDD[(String, (String, String))] = groupedRDD.flatMapValues(iter => {

      val sorted: List[String] = iter.toList.sorted
      var index = 1
      val format = new SimpleDateFormat("yyyy-MM-dd")
      val calendar: Calendar = Calendar.getInstance()

      sorted.map(x => {

        val date: Date = format.parse(x)
        calendar.setTime(date)
        calendar.add(Calendar.DATE, -index)
        index += 1
        (x, format.format(calendar.getTime))

      })

    })


    val result: RDD[(String, String, String, Int)] = flatmappedRDD.map(x => {

      ((x._1, x._2._2), x._2._1)
    })
      .groupByKey()
      .map(x => {

        val firstTime: String = x._2.toList.head
        val endTime: String = x._2.toList.last
        val count: Int = x._2.size
        (x._1._1,firstTime, endTime, count)

      })

    result.foreach(println)

    spark.stop()

  }

}
