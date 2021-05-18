package cn.doitedu.dw.etl

import org.apache.spark.sql.Row

case class AppActionBean(
                          val account: String,
                          val appid: String,
                          val appversion: String,
                          val carrier: String,
                          val deviceid: String,
                          val devicetype: String,
                          val eventid: String,
                          val ip: String,
                          val latitude: Double,
                          val longitude: Double,
                          val nettype: String,
                          val osname: String,
                          val osversion: String,
                          val releasechannel: String,
                          val resolution: String,
                          val sessionid: String,
                          val timestamp: Long,
                          val properties: Map[String, String],
                          var country: String = null,
                          var province: String = null,
                          var city: String = null,
                          var region: String = null
                        )


object BeanUtil {

  def rowToBean(row: Row): AppActionBean = {
    val account = row.getAs[String]("account")
    val appid = row.getAs[String]("appid")
    val appversion = row.getAs[String]("appversion")
    val carrier = row.getAs[String]("carrier")
    val deviceid = row.getAs[String]("deviceid")
    val devicetype = row.getAs[String]("devicetype")
    val eventid = row.getAs[String]("eventid")
    val ip = row.getAs[String]("ip")
    val latitude = row.getAs[Double]("latitude")
    val longitude = row.getAs[Double]("longitude")
    val nettype = row.getAs[String]("nettype")
    val osname = row.getAs[String]("osname")
    val osversion = row.getAs[String]("osversion")
    val releasechannel = row.getAs[String]("releasechannel")
    val resolution = row.getAs[String]("resolution")
    val sessionid = row.getAs[String]("sessionid")
    val timestamp = row.getAs[Long]("timestamp")
    val properties = row.getAs[Map[String, String]]("properties")


    AppActionBean(
      account,
      appid,
      appversion,
      carrier,
      deviceid,
      devicetype,
      eventid,
      ip,
      latitude,
      longitude,
      nettype,
      osname,
      osversion,
      releasechannel,
      resolution,
      sessionid,
      timestamp,
      properties
    )
  }


}