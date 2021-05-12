import java.io.{File, FileInputStream}

import org.apache.spark.sql.SparkSession
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object Ip2RegionSparkDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    import spark.implicits._


    val ds = spark.createDataset(Seq("221.218.212.95", "255.255.255.255", "221.218.212.0"))

    val dbFile: File = new File("dataware/data/ip2region/ip2region.db")
    val in: FileInputStream = new FileInputStream(dbFile)
    val dbBin: Array[Byte] = new Array[Byte](dbFile.length().toInt)
    in.read(dbBin)

    val bc = spark.sparkContext.broadcast(dbBin)

    val res = ds.mapPartitions(iter => {
      val byteArr = bc.value
      val searcher = new DbSearcher(new DbConfig(), byteArr)
      iter.map(ip => {
        val areaInfo = searcher.memorySearch(ip).toString
        (ip, areaInfo)
      })
    }).toDF("ip", "area")


    res.show(100, false)

    spark.close()

  }
}
