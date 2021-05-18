package cn.doitedu.dw.etl

import java.io.{File, FileInputStream}

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * app行为日志，ods层加工到dwd层
 * 主要需求：
 * 1. 清洗过滤
 * 2. json打平（properties事件属性字段不打平，作为一个hashmap类型整体存储）
 * 3. 地理位置集成（先用gps匹配，如果没有gps，用ip去匹配）
 * 4. 每条数据标记一个guid
 * 5. 数据规范化
 * 6. 新老访客标记
 */
object AppActionLogOdsToDwd {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
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

    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", "2")
      .enableHiveSupport()
      .appName("app行为日志ods层加工到dwd层")
      .master(args(0))
      .getOrCreate()

    import spark.implicits._


    // 加载T日的ods表分区
    val df = spark.read.table("ods.app_action_log").where(s"dt='${args(1)}'")

    // 清洗过滤
    val isempty = (s: String) => {
      StringUtils.isBlank(s)
    }
    spark.udf.register("isempty", isempty)

    val washed = df
      // 过滤掉日志中account及deviceid全为空的记录
      .where("!(isempty(deviceid) and isempty(account))")
      //（properties/eventid/sessionid 缺任何一个都不行）
      .where("properties is not null  and !isempty(eventid) and !isempty(sessionid)")

      // 有数据延迟到达
      .where(s"from_unixtime(cast(timestamp/1000 as bigint),'yyyy-MM-dd')='${args(1)}'")


    // TODO  SESSION分割

    // TODO 数据规范处理


    // 加载geohash码地理位置知识库
    val geoDictDF = spark.read.parquet("/dicts/geodicts")
    // 整理字典库为kv结构，并收集为单机hashmap集合
    val geodictMap = geoDictDF.rdd.map(row => {
      val geohash = row.getAs[String]("geohash")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val region = row.getAs[String]("region")
      (geohash, (province, city, region))
    }).collectAsMap()
    // 广播
    val bc1 = spark.sparkContext.broadcast(geodictMap)


    // 加载ip地址知识库(读本地磁盘方式）
    /*val dbFile: File = new File("dataware/data/ip2region/ip2region.db")
    val in: FileInputStream = new FileInputStream(dbFile)
    val dbBin: Array[Byte] = new Array[Byte](dbFile.length().toInt)
    in.read(dbBin)*/


    // 加载ip地址知识库(读HDFS方式）
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://master:8020/")
    val fs = FileSystem.get(conf)
    val in = fs.open(new Path("/dicts/ip2region/ip2region.db"))
    val dbFile: FileStatus = fs.listStatus(new Path("/dicts/ip2region/ip2region.db"))(0)

    val dbBin: Array[Byte] = new Array[Byte](dbFile.getLen.toInt)
    in.readFully(0L, dbBin)

    val bc2 = spark.sparkContext.broadcast(dbBin)


    // 地理位置维度集成
    val integrated = washed.rdd.map(row => {
      // 从广播变量中获取geo地理位置字典
      val geoDict = bc1.value

      // 从广播变量中获取ip地址知识库（字节数组）
      val ipDictBytes = bc2.value
      val ipSearcher = new DbSearcher(new DbConfig(), ipDictBytes)

      val bean = BeanUtil.rowToBean(row)

      var country: String = "中国"
      var province: String = null
      var city: String = null
      var region: String = null

      // 根据经纬度，查询省市区
      try {
        val geoCode = GeoHash.geoHashStringWithCharacterPrecision(bean.latitude, bean.longitude, 5)
        val area: (String, String, String) = geoDict.getOrElse(geoCode, (null, null, null))
        province = area._1
        city = area._2
        region = area._3
      } catch {
        case e: Exception => e.printStackTrace()
      }

      // 如果没有经纬度，根据IP地址查询省市区
      if (province == null) {
        val block = ipSearcher.memorySearch(bean.ip).toString
        val split = block.split("\\|")
        // 2163|中国|华南|广东省|深圳市|鹏博士
        if (split.size > 4) {
          country = split(1)
          province = split(3)
          city = split(4)
        }
      }


      if (country.equals("0")) {
        bean.country = "内网国"
        bean.province = "内网省"
        bean.city = "内网市"
        bean.region = "内网区"
      } else {
        bean.country = country
        bean.province = if (province != null && province.equals("0")) null else province
        bean.city = if (city != null && city.equals("0")) null else city
        bean.region = if (region != null && region.equals("0")) null else region
      }

      bean
    }).toDF()


    // ID_MAPPING
    // 加载设备id账号绑定评分表  :  此时，一个设备id可能有多条用户账号的绑定得分
    val bindScore = spark.read.table("dwd.id_account_bind").where(s"dt='${args(1)}'")

    // 加工成： 一个设备id对应一个权重最高的guid
    // t.deviceid   | t.account  | t.first_login_ts  | t.bind_score  |
    import org.apache.spark.sql.functions._
    val wd = Window.partitionBy('deviceid).orderBy('bind_score desc, 'first_login_ts asc)
    val bindDictDF = bindScore
      .select('deviceid, 'account, row_number() over (wd) as "rn")
      .where("rn=1")
      .selectExpr("deviceid", "nvl(account,deviceid) as guid")

    // 将处理好的日志数据  和   设备账号绑定映射表   JOIN
    integrated.createTempView("log")
    bindDictDF.createTempView("bind")
    val guidDF = spark.sql(
      """
        |
        |select
        |log.account         ,
        |log.appid           ,
        |log.appversion      ,
        |log.carrier         ,
        |log.deviceid        ,
        |log.devicetype      ,
        |log.eventid         ,
        |log.ip              ,
        |log.latitude        ,
        |log.longitude       ,
        |log.nettype         ,
        |log.osname          ,
        |log.osversion       ,
        |log.releasechannel  ,
        |log.resolution      ,
        |log.sessionid       ,
        |log.timestamp       ,
        |log.properties      ,
        |log.country         ,
        |log.province        ,
        |log.city            ,
        |log.region          ,
        |bind.guid
        |from log  join  bind on log.deviceid=bind.deviceid
        |
        |""".stripMargin)

    // 新老访客标记,先加载T-1日的绑定评分表
    val ids: DataFrame = spark.read.table("dwd.id_account_bind")
      .where(s"dt='${args(2)}'")
      .select(explode(array("deviceid", "account")) as "id")
      .distinct()

    // 将前面处理好的日志数据  JOIN  IDS表
    guidDF.createTempView("log_guid")
    ids.createTempView("ids")
    spark.sql(
      s"""
         |insert into dwd.app_action_detail partition(dt='${args(1)}')
         |select
         |   a.account         ,
         |   a.appid           ,
         |   a.appversion      ,
         |   a.carrier         ,
         |   a.deviceid        ,
         |   a.devicetype      ,
         |   a.eventid         ,
         |   a.ip              ,
         |   a.latitude        ,
         |   a.longitude       ,
         |   a.nettype         ,
         |   a.osname          ,
         |   a.osversion       ,
         |   a.releasechannel  ,
         |   a.resolution      ,
         |   a.sessionid       ,
         |   a.timestamp as ts ,
         |   a.properties      ,
         |   a.country         ,
         |   a.province        ,
         |   a.city            ,
         |   a.region          ,
         |   a.guid            ,
         |   if(b.id is not null,0,1) as isnew
         |from log_guid a left join ids b on a.guid = b.id
         |
         |""".stripMargin)

    spark.close()

  }
}
