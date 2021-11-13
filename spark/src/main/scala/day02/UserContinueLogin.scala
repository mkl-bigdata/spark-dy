package day02

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserContinueLogin {

  private val comma = ","

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    val spark = new SparkContext(conf)

    val inputData: RDD[String] = spark.textFile("data/user")

    //处理输入数据
    val value: RDD[(String, String)] = inputData.map(x => {

      val fields: Array[String] = x.split(comma)
      (fields(0), fields(1))

    })

    //找出同一个guid的登录日期
    val guid_loginDate: RDD[(String, (String, String))] = value.groupByKey()
      .flatMapValues(iter => {

        //每个人按照日期进行排序
        val sorted: List[String] = iter.toSet.toList.sorted
        //定义一个日期类
        val calendar: Calendar = Calendar.getInstance()
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var index = 0

        sorted.map(x => {
          val date: Date = dateFormat.parse(x)
          calendar.setTime(date)
          calendar.add(Calendar.DATE, -index)
          index += 1
          //得到当前行日期减去当前行的索引值
          (x, dateFormat.format(calendar.getTime))
        })
      })


    val result: RDD[(String, Int, String, String)] = guid_loginDate.map { case (guid, (logindate, logindatesub)) => {

      ((guid, logindatesub), logindate)

    }
    }
      .groupByKey()
      .map { case ((guid, logindatesub), iter) => {

        val sorted: List[String] = iter.toSet.toList.sorted
        val times: Int = iter.size
        val firstTime: String = sorted.head
        val endTime: String = sorted.last

        (guid, times, firstTime, endTime)


      }
      }


    println(result.collect().toBuffer)

    spark.stop()

  }
}
