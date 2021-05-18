package cn.doitedu.dw.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * 设备id和登录账号绑定关系计算
 * 每天滚动合并更新
 *
 * 绑定评分表的建表语句：
 * DROP TABLE IF EXISTS dwd17.`id_account_bind`;
 * CREATE TABLE dwd17.`id_account_bind`(
 * `deviceid` string,
 * `account` string,
 * `first_login_ts` bigint,
 * `bind_score` double
 * )
 * partitioned by (dt string)
 * stored as parquet
 * ;
 *
 *
 * 生产部署，要放在yarn上运行，
 *
 * ### 提交任务时，需要指定要申请的资源量
 * ${SPARK_HOME}/bin/spark-submit \
 * --master yarn \
 * --deploy-mode cluster \
 * --class cn.doitedu.dw.util.IdBind \
 * --name id_bind_calc \
 * --driver-memory 1024M \
 * --executor-memory 2G \
 * --queue default \
 * --num-executors 2 idbind.jar yarn $DT_CACL $DT_HIST
 *
 * ### yarn有一系列资源配置限制
 * yarn.resourcemanager.scheduler.class	org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler	The class to use as the resource scheduler.
 * yarn.scheduler.minimum-allocation-mb	      1024	 每个容器分配内存的最小值
 * yarn.scheduler.maximum-allocation-mb	      8192	 每个容器分配内存的最大值
 * yarn.scheduler.minimum-allocation-vcores	  1	     每个容器分配cpu核数的最小值
 * yarn.scheduler.maximum-allocation-vcores	  4	     每个容器分配cpu核数的最大值
 * yarn.nodemanager.resource.memory-mb        8g     每个nodemanager上的内存资源总量
 * yarn.nodemanager.resource.cpu-vcores       8core  每个nodemanager上的cpu核数资源总量
 *
 *
 */
object IdBind {
  def main(args: Array[String]): Unit = {

    if(args.length<3){
      println(
        """
          |Error parameter number
          |Usage:
          |   args(0)  运行模式
          |   args(1)  T日期
          |   args(2)  T-1日
          |
          |""".stripMargin)
      sys.exit(2)
    }


    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","2")
      .enableHiveSupport() // 开启hive整合支持（同时，需要引入spark-hive的依赖；引入hadoop和hive的配置文件）
      .appName("地理位置知识库加工")
      .master(args(0))
      .getOrCreate()

    // 加载T日日志数据
    val logDf = spark.read.table("ods.app_action_log").where(s"dt='${args(1)}'")
    logDf.createTempView("logdf")

    // 计算T日的 设备->账号  绑定得分
    val loginCnts = spark.sql(
      """
        |
        |select
        |deviceid,
        |if(account is null or trim(account)='',null,account) as account,
        |-- count(distinct sessionid) as login_cnt,
        |min(timestamp) as first_login_ts,
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
    val bindScorePre = spark.read.table("dwd.id_account_bind").where(s"dt='${args(2)}'")

    println("历史评分结果")
    bindScorePre.show(100)
    bindScorePre.createTempView("yestoday")

    // 全外关联两个绑定得分表
    // 并将结果写入hive表的当天分区（T-1日分区就无用了）
    val combined = spark.sql(
      s"""
         |
         |insert into table dwd.id_account_bind partition(dt='${args(1)}')
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