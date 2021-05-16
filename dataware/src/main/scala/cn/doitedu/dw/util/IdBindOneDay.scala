package cn.doitedu.dw.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * 设备id与登录之间的绑定关系计算
 * 逻辑试验用
 * 只考虑当天的数据
 */
object IdBindOneDay {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "2")
      .enableHiveSupport()
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val schema = new StructType()
      .add("account", DataTypes.StringType)
      .add("deviceid", DataTypes.StringType)
      .add("sessionid", DataTypes.StringType)
      .add("ts", DataTypes.LongType)
    val logDf = spark.read.option("header", "true").schema(schema)
      //.csv("dataware/data/idbind/input/day01")  // 本地路径
      .csv("/idbind/input/day01") //hdfs路径

    logDf.createTempView("logdf")
    val loginCnts = spark.sql(
      """
        |
        |insert into table dwd.id_account_bind partition(dt='2020-10-06')
        |select
        |deviceid,
        |account,
        |min(ts) as first_login_ts,
        |count(distinct sessionid)*100 as bind_score
        |from logdf
        |group by deviceid,account
        |
        |""".stripMargin)

    spark.close()


  }
}
