package cn.doitedu.dw.util

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * 经纬度地理位置知识库，加工成geohash码地理位置知识库
 */
object GeoHashDict {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()

    import spark.implicits._

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

    
    spark.close()
  }
}
