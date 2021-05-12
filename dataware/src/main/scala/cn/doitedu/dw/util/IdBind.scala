package cn.doitedu.dw.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}


object IdBind {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "2")
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = new StructType()
      .add("account", DataTypes.StringType)
      .add("deviceid", DataTypes.StringType)
      .add("sessionid", DataTypes.StringType)
      .add("ts", DataTypes.StringType)

    val logDf = spark.read.option("header", "true").csv("dataware/data/idbind/input/day01")
    logDf.cache()
    //logDf.show(100, false)

    logDf.createTempView("logdf")
    val loginCnt = spark.sql(
      """
        |
        |select
        |deviceid,account,count(distinct sessionid) as login_cnt,min(ts) as first_login_ts,count(distinct sessionid)*100 as bind_score
        |from logdf
        |group by deviceid,account
        |
        |""".stripMargin)
    //loginCnt.show(100, false)

    loginCnt.coalesce(1).write.parquet("dataware/data/idbind/output/day01")

    spark.close()

  }

}
