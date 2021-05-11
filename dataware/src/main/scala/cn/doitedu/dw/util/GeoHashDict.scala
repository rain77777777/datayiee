package cn.doitedu.dw.util

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * 经纬度地理位置知识库，加工成geohash码地理位置知识库
 */
object GeoHashDict {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","2")
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    // 加载mysql中的原始数据
    val props = new Properties()
    props.load(GeoHashDict.getClass.getClassLoader.getResourceAsStream("db.properties"))

    val df = spark.read.jdbc("jdbc:mysql://localhost:3306/realtimedw?useUnicode=true&charcterEncoding=utf8", "t_md_areas", props)

    //扁平化处理
    df.createTempView("df")
    val df2 = spark.sql(
      """
        |
        |select
        |province.areaname as province,
        |city.areaname as city,
        |region.areaname as region,
        |region.bd09_lng  as lng,
        |region.bd09_lat  as lat
        |from df region join df city on region.parentid = city.id and region.level=3
        |               join df province on city.parentid = province.id
        |
        |""".stripMargin)


    df2.show(20, false)


    val gps2geo:UserDefinedFunction = udf((lat: Double, lng: Double) => {
      GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 5)
    })

    val result = df2.select('province, 'city, 'region, gps2geo('lat, 'lng) as "geohassh")

    result.write.parquet("dataware/data/geodict")

    spark.close()

  }
}
