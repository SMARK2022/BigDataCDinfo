## 项目名称

BigDataCDinfo - 基于大数据技术的新冠疫情防控

## 项目描述

本项目由HITSZ22级数据科学专业大二本科生完成的大数据计算基础课程项目，旨在利用大数据技术支持新冠疫情的控制与管理。通过分析手机漫游信息和感染者信息，我们找出了与感染者有接触风险的人员，并将其标注为红码，以促使密切接触者进行核酸筛查。

## 贡献者

- SMARK
- lampethereal
- xt, xz, rb, hz, xy. (缩写)

## 文件结构

```
BigDataCDinfo
├─ 大作业方案汇报.pdf
├─ Dask方案.ipynb
├─ demo.ipynb
├─ README.md
├─ Strange5273 (考虑了其他可能的数据缺陷与非配对数据对程序造成的影响、未通过Debug但更完善)
│  ├─ .idea
│  ├─ out
│  └─ src
│     └─ Strange5273.scala
└─ Update5273 (最终项目)
   ├─ .idea
   ├─ out
   └─ src
      └─ Update5273.scala
```

## 使用技术

- Apache Spark
- Scala
- Hadoop
- HDFS

## 项目背景

本次实验要求从下发数据中找到需要标红码的人员列表，即与感染人员同时间在一个基站的手机列表，并按号码升序导出到 `redmark+组号.txt` 文件中。通过本项目，我们旨在通过分析手机漫游数据，找出潜在的密切接触者，并为新冠疫情的防控提供技术支持。

## 核心代码说明

### 1. 读入数据

```scala
val cdinfo = spark.read.csv(args(0)).toDF("id", "timestamp", "type", "personId")
val infected = spark.read.csv(args(1)).toDF("personId")
```

### 2. 提取感染人群手机号

```scala
val infectedPersonIds = infected.select("personId").as[String].collect().toSet
val infectedPersonIdsBC = spark.sparkContext.broadcast(infectedPersonIds)
```

### 3. 过滤数据

```scala
val filteredCdinfo = cdinfo.filter(row => infectedPersonIdsBC.value.contains(row.getString(3)))
```

### 4. 分组并提取基站内的感染时间

```scala
val groupedCdinfo = filteredCdinfo.groupBy("id").agg(
  collect_list(when(col("type") === 1, col("timestamp")).cast("long")).alias("startTimes"),
  collect_list(when(col("type") === 2, col("timestamp")).cast("long")).alias("endTimes")
)
```

### 5. 数据持久化

```scala
groupedCdinfo.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### 6. 广播感染时间

```scala
val baseStationInfectedTimes: Map[String, (Seq[Long], Seq[Long])] = groupedCdinfo.collect().map { row =>
  val baseId = row.getString(0)
  val startTimes = row.getAs[Seq[Long]]("startTimes")
  val endTimes = row.getAs[Seq[Long]]("endTimes")
  (baseId, (startTimes, endTimes))
}.toMap

val baseStationInfectedTimesBC = spark.sparkContext.broadcast(baseStationInfectedTimes)
```

### 7. 筛选潜在感染者

```scala
val potentiallyInfectedPeople = cdinfo.filter { row =>
  val baseId = row.getString(0)
  baseStationInfectedTimesBC.value.contains(baseId)
}

potentiallyInfectedPeople.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### 8. 判断感染与否的函数

```scala
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
```

### 9. 注册UDF函数并筛选最终感染者

```scala
val isInfectedUDF = udf(isInfected _)
val finalInfected = groupedPotentiallyInfected.filter(isInfectedUDF(col("id"), col("times")))
finalInfected.show()
```

### 10. 输出结果

```scala
val basicTask = potentiallyInfectedPeople.select("personId").distinct().sort("personId")
basicTask.coalesce(1).write.text(args(2))

val finalTask = finalInfected.select("personId").distinct().sort("personId")
finalTask.coalesce(1).write.text(args(3))
```

## 执行步骤

### 准备工作

1. 克隆项目：
   ```bash
   git clone [项目地址]
   ```

2. 在IDEA中打开项目，并修改 `Update5273` 的配置，编译项目并打包为 JAR 文件。

### 正式执行

1. 下载并解压数据。

2. 将数据上传至HDFS：
   ```bash
   hdfs dfs -mkdir /Bigdata

   # 上传文件到HDFS
   hdfs dfs -put ~/Bigdata/cdinfo.txt /Bigdata/cdinfo.txt
   hdfs dfs -put ~/Bigdata/infected.txt /Bigdata/infected.txt
   ```

3. 使用Spark进行数据处理：
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --class Update5273 \
     --conf spark.default.parallelism=120 \
     --conf spark.executor.memory=4g \
     --conf spark.executor.cores=4 \
     --conf spark.cores.max=96 \
     --num-executors 5 \
     ~/IDEAout/Update5273.jar \
     hdfs:///Bigdata/cdinfo.txt \
     hdfs:///Bigdata/infected.txt \
     redmark_final.txt \
     superredmark_final.txt
   ```

4. 在HDFS上下载结果并提交。

## 总结

在本次大作业中，我们小组自行搭建了Spark集群，利用了组内五名成员的电脑作为集群节点进行数据处理。通过多次模拟测试和调试，我们确保了代码的稳定性与正确性。项目中，我们不仅丰富了Spark相关知识，还提升了实践能力和团队合作精神。

## 感谢

- 感谢20、21级学姐学长的以往代码，我们在其基础上完成的更新与修改，实现了更高效与充分的性能提升。
- 感谢老师的指导，帮助我们顺利完成本次大作业。

## 参考

- [Spark性能优化指南——基础篇](https://tech.meituan.com/2016/04/29/spark-tuning-basic.html)
- [Spark性能优化指南——高级篇](https://tech.meituan.com/2016/05/12/spark-tuning-pro.html)