import org.apache.spark.sql.functions.{col, collect_list, when, udf}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Strange5273 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First2022_update")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val cdinfo = spark.read.csv(args(0)).toDF("id", "timestamp", "type", "personId")
    val infected = spark.read.csv(args(1)).toDF("personId")

    val infectedPersonIds = infected.select("personId").as[String].collect().toSet
    val infectedPersonIdsBC = spark.sparkContext.broadcast(infectedPersonIds)

    // 过滤每个分区的数据，减少数据传输
    val filteredCdinfo = cdinfo.filter(row => infectedPersonIdsBC.value.contains(row.getString(3)))

    val groupedCdinfo = filteredCdinfo.groupBy("id").agg(
      collect_list(when(col("type") === 1, col("timestamp")).cast("long")).alias("startTimes"),
      collect_list(when(col("type") === 2, col("timestamp")).cast("long")).alias("endTimes")
    )

    // 持久化 groupedCdinfo 数据
    groupedCdinfo.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 显式指定 collect 后的类型，并确保 startTimes 和 endTimes 长度一致
    val baseStationInfectedTimes: Map[String, (Seq[Long], Seq[Long])] = groupedCdinfo.collect().map { row =>
      val baseId = row.getString(0)
      val startTimes = row.getAs[Seq[Long]]("startTimes")
      var endTimes = row.getAs[Seq[Long]]("endTimes")
      if (startTimes.length != endTimes.length) {
        endTimes = endTimes.drop(endTimes.length - startTimes.length)
      }
      (baseId, (startTimes, endTimes))
    }.toMap

    val baseStationInfectedTimesBC = spark.sparkContext.broadcast(baseStationInfectedTimes)

    val potentiallyInfectedPeople = cdinfo.filter { row =>
      val baseId = row.getString(0)
      baseStationInfectedTimesBC.value.contains(baseId)
    }

    potentiallyInfectedPeople.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val groupedPotentiallyInfected = potentiallyInfectedPeople.groupBy("id", "personId").agg(
      collect_list(col("timestamp").cast("long")).alias("times")
    )
    groupedPotentiallyInfected.show(false)

    def isInfected(baseId: String, times: Seq[Long]): Boolean = {
      val sortedTimes = times.sorted
      // 如果 times 长度为奇数，直接返回 true
      if (sortedTimes.length % 2 != 0) {
        return true
      }

      if (sortedTimes.nonEmpty && baseStationInfectedTimesBC.value.contains(baseId)) {
        val (startTimes, endTimes) = baseStationInfectedTimesBC.value(baseId)
        for (i <- startTimes.indices) {
          val startTime = startTimes(i)
          val endTime = endTimes(i)
          for (j <- sortedTimes.indices by 2) {
            val entryTime = sortedTimes(j)
            val exitTime = sortedTimes(j + 1)
            if ((startTime <= entryTime && endTime >= entryTime) ||
              (startTime <= exitTime && endTime >= exitTime) ||
              (startTime >= entryTime && endTime <= exitTime)) {
              return true
            }
          }
        }
      }
      false
    }


    def isInfected(baseId: String, times: Seq[Long]): Boolean = {
      val sortedTimes = times.sorted
      if (sortedTimes.nonEmpty && baseStationInfectedTimesBC.value.contains(baseId)) {
        val (startTimes, endTimes) = baseStationInfectedTimesBC.value(baseId)
        for (i <- startTimes.indices) {
          val startTime = startTimes(i)
          val endTime = endTimes(i)
          for (j <- sortedTimes.indices by 2) {
            if (j + 1 < sortedTimes.length) {
              val entryTime = sortedTimes(j)
              val exitTime = sortedTimes(j + 1)
              if ((startTime <= entryTime && endTime >= entryTime) ||
                (startTime <= exitTime && endTime >= exitTime) ||
                (startTime >= entryTime && endTime <= exitTime)) {
                return true
              }
            }
          }
        }
      }
      false
    }


    // 注册UDF函数
    val isInfectedUDF = udf(isInfected _)
    val finalInfected = groupedPotentiallyInfected.filter(isInfectedUDF(col("id"), col("times")))
    finalInfected.show()

    val basicTask = potentiallyInfectedPeople.select("personId").distinct().sort("personId")
    basicTask.coalesce(1).write.text(args(2))

    val finalTask = finalInfected.select("personId").distinct().sort("personId")
    finalTask.coalesce(1).write.text(args(3))

    groupedCdinfo.unpersist()
    potentiallyInfectedPeople.unpersist()
  }
}