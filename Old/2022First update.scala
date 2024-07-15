import org.apache.spark.sql.functions.{col, collect_list, struct, when, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object First2022_update {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First2022_update")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") // 序列化优化
      .getOrCreate()

    val cdinfo = spark.read.csv(args(0))
    val infected = spark.read.csv(args(1))
    val infectedPersonIds = infected.select("_c0").rdd.map(row => row(0).toString).collect().distinct

    val filteredCdinfo = cdinfo.filter(col("_c3").isin(infectedPersonIds: _*))

    val groupedCdinfo = filteredCdinfo.groupBy("_c0").agg(
      collect_list(when(col("_c2") === 1, col("_c1")).cast("long")).alias("startTimes"),
      collect_list(when(col("_c2") === 2, col("_c1")).cast("long")).alias("endTimes")
    )

    // 持久化 groupedCdinfo 数据
    groupedCdinfo.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 显式指定 collect 后的类型
    val baseStationInfectedTimes: Map[String, (Seq[Long], Seq[Long])] = groupedCdinfo.collect().map { row =>
      val baseId = row.getString(0)
      val startTimes = row.getAs[Seq[Long]]("startTimes").filter(_ != null)
      val endTimes = row.getAs[Seq[Long]]("endTimes").filter(_ != null)
      (baseId, (startTimes, endTimes))
    }.toMap


    val infectedBaseStations = baseStationInfectedTimes.keys.toList
    val potentiallyInfectedPeople = cdinfo.filter(col("_c0").isin(infectedBaseStations: _*))
    val groupedPotentiallyInfected = potentiallyInfectedPeople.groupBy("_c0", "_c3").agg(
      collect_list("_c1").cast("long").alias("times")
    )
    groupedPotentiallyInfected.show(false)

    def isInfected(baseId: String, times: Seq[Long]): Boolean = {
      val sortedTimes = times.sorted
      if (sortedTimes.nonEmpty && baseStationInfectedTimes.contains(baseId)) {
        val (startTimes, endTimes) = baseStationInfectedTimes(baseId)
        for (i <- startTimes.indices) {
          val startTime = startTimes(i)
          val endTime = endTimes(i)
          if ((startTime <= sortedTimes.head && endTime >= sortedTimes.head) ||
            (startTime <= sortedTimes.last && endTime >= sortedTimes.last) ||
            (startTime >= sortedTimes.head && endTime <= sortedTimes.last)) {
            return true
          }
        }
      }
      false
    }

    // 注册UDF函数
    val isInfectedUDF = udf(isInfected _)
    val finalInfected = groupedPotentiallyInfected.filter(isInfectedUDF(col("_c0"), col("times")))
    finalInfected.show()

    val basicTask = potentiallyInfectedPeople.select("_c3").distinct().sort("_c3")
    basicTask.coalesce(1).write.text(args(2))

    val finalTask = finalInfected.select("_c3").distinct().sort("_c3")
    finalTask.coalesce(1).write.text(args(3))

    groupedCdinfo.unpersist()
  }
}
