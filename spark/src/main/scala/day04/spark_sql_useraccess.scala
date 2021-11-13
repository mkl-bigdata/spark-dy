package day04

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

object spark_sql_useraccess {

  case class user(uid:String,acc_time:String)

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


        val inputRDD: RDD[user] = spark.sparkContext.textFile("data/data1.txt")
          .map(x => {

            val fields: Array[String] = x.split(" ")

            user(fields(0),fields(1))

          })

        import spark.implicits._

        val inputDF: DataFrame = inputRDD.toDF()

        inputDF.createTempView("v_log")

        //inputDF.show()

        val result: DataFrame = spark.sql(
          """
            |select
            |uid,
            |min(acc_time),
            |max(acc_time),
            |count(*) as c
            |from
            |(
            |
            |	select
            |	uid,
            |	acc_time,
            |	date_sub(acc_time,rank) as d_s
            |	from
            |	(
            |		select
            |		uid,
            |		acc_time,
            |		row_number() over(partition by uid order by acc_time asc) as rank
            |		from
            |		(
            |			select
            |			uid,
            |			acc_time
            |			from v_log
            |
            |		)
            |
            |)
            |) group by uid,d_s having c >= 3
            |
            |
            |""".stripMargin)


        result.show()

        spark.stop()


  }

}
