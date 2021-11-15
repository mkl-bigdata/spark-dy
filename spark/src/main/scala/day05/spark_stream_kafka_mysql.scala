package day05

import java.sql.{Connection, PreparedStatement, ResultSet}

import day05.spark_stream_kafka_mysql.{appid, gid}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object spark_stream_kafka_mysql {

  private val gid = "g03"

  private val appid = this.getClass.getSimpleName


  val kafkaParams = Map[String,Object](


    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> gid,
    "auto.offset.reset" -> "earliest",
  "enable.auto.commit" -> (false: java.lang.Boolean)

  )

  val topic = Array("wordcount")



  /**
   * 把更新值和偏移量的方法写到一个事务中，要成功都成功，否则回滚
   * @param: [spark, appid, gid]
   * @return: void
   * @author: makuolang
   * @date: 2021/11/15
   */
  def kafka_2_mysql(spark:StreamingContext,appid:String,gid:String): Unit ={

    val map: mutable.Map[TopicPartition, Long] = getOffset(spark,appid, gid)

    //获取偏移量
    val inputstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      spark,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topic, kafkaParams, map)

    )

    //更新操作，并包装到事务当中
    inputstream.foreachRDD(rdd => {

      if(!rdd.isEmpty()){


        //拿到偏移量
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿到数据库连接
        val connection: Connection = DruidConnectionPool.getConnection
        connection.setAutoCommit(false)

        //进行更新
        val result: Array[(String, Int)] = rdd.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()

        val statement: PreparedStatement = connection.prepareStatement("insert into wordcount(word,count) values(?,?) ON DUPLICATE KEY UPDATE count = count + ?")

        for (elem <- result) {


          statement.setString(1,elem._1)
          statement.setInt(2,elem._2)
          statement.setInt(3,elem._2)


          statement.executeUpdate()
        }

        //把偏移量写到mysql中维护
        val statement2: PreparedStatement = connection.prepareStatement("insert into offset(appid_gid,topic_partition,offset) values(?,?,?) ON DUPLICATE KEY UPDATE offset = ?")

        for (range <- ranges) {

          val topic = range.topic
          val partition = range.partition
          val offset = range.untilOffset

          //设置参数
          statement2.setString(1, appid + "_" + gid)
          statement2.setString(2, topic + "_" + partition)
          statement2.setLong(3, offset)
          statement2.setLong(4, offset)

          //执行update
          statement2.executeUpdate()
        }

        connection.commit()
      }

      })}
  /**
   * 获取对应appid，gid的偏移量的方法
   * @param: [spark, appid, gid]
   * @return: scala.collection.mutable.Map<org.apache.kafka.common.TopicPartition,java.lang.Object>
   * @author: makuolang
   * @date: 2021/11/15
   */

  def getOffset(spark:StreamingContext,appid:String,gid:String): mutable.Map[TopicPartition, Long] ={

    val map = new mutable.HashMap[TopicPartition, Long]()

    var connection: Connection = null

    var statement: PreparedStatement = null

    try {
      connection = DruidConnectionPool.getConnection

      statement = connection.prepareStatement("select topic_partition,offset from offset where appid_gid = ?")

      statement.setString(1, appid + "_" + gid)

      val result: ResultSet = statement.executeQuery()

      while (result.next()) {


        val fields: Array[String] = result.getString(1).split("_")

        val topic: String = fields(0)

        val partition: Int = fields(1).toInt

        val t_p = new TopicPartition(topic, partition)

        val offset: Int = result.getInt(2)

        map(t_p) = offset

      }
    } catch {
      case e : Exception => {

        e.printStackTrace()

        //出现错误，进行回滚
        connection.rollback()

        //停掉程序
        spark.stop()

      }
    } finally {

      if(statement != null){

        statement.close()

      }

      if(connection != null){

        connection.close()
      }

    }

      map

  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(appid).setMaster("local[*]")

    val spark = new StreamingContext(conf, Seconds(5))

    spark.sparkContext.setLogLevel("WARN")

    kafka_2_mysql(spark,appid,gid)


    spark.start()

    spark.awaitTermination()


  }

}
