package cn.doitedu.dw.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * 设备id和登录账号绑定关系计算
 * 每天滚动合并更新
 */
object IdBindRollCombine {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","2")
      .enableHiveSupport() // 开启hive整合支持（同时，需要引入spark-hive的依赖；引入hadoop和hive的配置文件）
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()

    val schema = new StructType()
      .add("account",DataTypes.StringType)
      .add("deviceid",DataTypes.StringType)
      .add("sessionid",DataTypes.StringType)
      .add("ts",DataTypes.LongType)

    // 加载T日日志数据
    val logDf = spark.read.option("header","true").schema(schema).csv("/idbind/input/day02")
    logDf.createTempView("logdf")

    // 计算T日的 设备->账号  绑定得分
    val loginCnts = spark.sql(
      """
        |
        |select
        |deviceid,
        |account,
        |-- count(distinct sessionid) as login_cnt,
        |min(ts) as first_login_ts,
        |count(distinct sessionid)*100 as bind_score
        |from logdf
        |group by deviceid,account
        |
        |""".stripMargin)
    loginCnts.createTempView("today")

    println("当天评分结果")
    loginCnts.show(100)

    // 加载 T-1的 绑定得分  （从hive的绑定评分表中加载）
    // val bindScorePre = spark.read.parquet("dataware/data/idbind/output/day01")
    val bindScorePre = spark.read.table("dwd.id_account_bind").where("dt='2020-10-06'")

    println("历史评分结果")
    bindScorePre.show(100)
    bindScorePre.createTempView("yestoday")

    // 全外关联两个绑定得分表
    // 并将结果写入hive表的当天分区（T-1日分区就无用了）
    val combined = spark.sql(
      """
        |
        |insert into table dwd.id_account_bind partition(dt='2020-10-07')
        |
        |select
        |if(today.deviceid is null,yestoday.deviceid,today.deviceid) as deviceid,
        |if(today.account is null,yestoday.account,today.account) as account,
        |if(yestoday.first_login_ts is not null,yestoday.first_login_ts,today.first_login_ts) as first_login_ts,
        |-- if(today.account is null,yestoday.login_cnt,today.login_cnt+yestoday.login_cnt) as login_cnt,
        |if(today.account is null,yestoday.bind_score*0.9,today.bind_score+if(yestoday.bind_score is null,0,yestoday.bind_score)) as bind_score
        |from
        |  today
        |full join
        |  yestoday
        |on today.deviceid=yestoday.deviceid and today.account=yestoday.account
        |
        |""".stripMargin)

    spark.close()

  }

}
