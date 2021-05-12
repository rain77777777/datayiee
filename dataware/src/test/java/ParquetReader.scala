import org.apache.spark.sql.SparkSession

object ParquetReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions","2")
      .appName("地理位置知识库加工")
      .master("local")
      .getOrCreate()

    val df = spark.read.load("dataware/data/geodict")
    df.show(100,false)
    spark.close()
    
  }

}
