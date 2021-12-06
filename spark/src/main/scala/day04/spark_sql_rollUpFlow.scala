package day04

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_sql_rollUpFlow {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.csv("spark/data/data.csv").toDF("uid", "start_time", "end_time", "flow")

    df.createTempView("v_flow")

    spark.sql(
      """
        |
        |select
        |    uid,
        |    min(start_time),
        |    max(end_time),
        |    sum(flow)
        |
        |from
        |(
        |    select
        |
        |    uid,
        |    start_time,
        |    end_time,
        |    sum(date_diff) over(partition by uid order by start_time) diff_sum,
        |    flow
        |
        |from
        |
        |(
        |
        |select
        |
        |    uid,
        |    start_time,
        |    end_time,
        |    if((unix_timestamp(start_time) -  unix_timestamp(flag)) / 60 < 10 ,0,1) date_diff,
        |    flow
        |
        |from
        |(
        |    select
        |
        |        uid,
        |        start_time,
        |        end_time,
        |        lag(end_time,1,start_time) over(partition by uid order by start_time) flag,
        |        flow
        |
        |    from
        |
        |    v_flow
        |)
        |
        |)
        |
        |) group by uid,diff_sum
        |
        |""".stripMargin).show()


    spark.stop()


  }

}
