package day03

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object iP2province {

  private val inputPath = "/Users/mkl/Downloads/ipaccess.log"

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark = new SparkContext(conf)

    val inputRDD: RDD[String] = spark.textFile("data/ip.txt")

    val ipmapped: Array[(Long, Long, String, String)] = inputRDD.map(x => {

      val fields: Array[String] = x.split("[|]")

      val startip: Long = fields(2).toLong
      val endip: Long = fields(3).toLong
      val province: String = fields(6)
      val city: String = fields(7)

      (startip, endip, province, city)

    }).collect()

    val broadcastRDD: Broadcast[Array[(Long, Long, String, String)]] = spark.broadcast(ipmapped)


    val iplogs: RDD[String] = spark.textFile(inputPath)

    val ip2pro: RDD[(String, Int)] = iplogs.map(x => {

      val fields: Array[String] = x.split("[|]")
      val ip: String = fields(1)
      val ip2ten: Long = IpUtils.ip2Long(ip)

      val bc: Array[(Long, Long, String, String)] = broadcastRDD.value

      val index: Int = IpUtils.binarySearch(bc, ip2ten)

      var province = "未知"
      if (index != -1) {
        province = bc(index)._4
      }

      (province, 1)
    })

    val result: RDD[(String, Int)] = ip2pro.reduceByKey(_ + _)

    println(result.collect().toBuffer)

    spark.stop()



  }

}
