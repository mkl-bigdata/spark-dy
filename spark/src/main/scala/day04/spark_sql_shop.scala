package day04

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object spark_sql_shop {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .csv("data/shop.csv")


    df.createTempView("v_shop")

   spark.sql(
     """
       |
       |select
       |
       |	sid,
       |	mt,
       |	s_m,
       |	sum(s_m) over(partition by sid order by mt asc) shop_money
       |
       |from
       |
       |(
       |	select
       |		sid,
       |		mt,
       |		sum(money) s_m
       |
       |	from
       |	(
       |		select
       |			sid
       |			,date_format(dt,"yyyy-MM") mt
       |			,money
       |		from
       |		v_shop
       |
       |	) group by sid,mt
       |
       |)
       |
       |""".stripMargin).show()


    spark.stop()


  }

}
