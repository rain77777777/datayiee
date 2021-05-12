import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object Ip2RegionDemo {
  def main(args: Array[String]): Unit = {
    val searcher = new DbSearcher(new DbConfig(), "dataware/data/ip2region/ip2region.db")

    val block1: DataBlock = searcher.memorySearch("221.218.212.95")
    val block2: DataBlock = searcher.memorySearch("255.255.255.255")
    val block3: DataBlock = searcher.memorySearch("221.218.212.0")
    val block4: DataBlock = searcher.memorySearch("192.168.33.11")
    val block5: DataBlock = searcher.memorySearch("8.8.8.8")

    println(block1)
    println(block2)
    println(block3)
    println(block4)
    println(block5)
  }
}
