package Demo

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object FindRed {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("FindRed")
    var samp = 0.05
    if(args.length==4){conf = conf.setMaster("local");samp=1}
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val starttime: Long = System.currentTimeMillis()
    // RDD处理
    val infected = args(0)
    val cdinfo = args(1)
    val output = args(2)
    val datapre = sc.textFile(cdinfo)
      .sample(withReplacement = false, samp)
      .map(x=>{
        val line = x.split(",")
        if(line(2).toInt==1)
          (line(0), List((line(3),line(1), 1))) //(基站id,手机号,时间戳,状态)
        else
          (line(0), List((line(3),line(1), -1)))
      })
      .reduceByKey((x,y)=>x++y) //按基站id分组
      .map(x => x._2
        .toArray
        .sortBy(_._2)
        .map(x=>(x._1,x._3))
        )
      .persist(StorageLevel.MEMORY_AND_DISK_SER)  //persist并设定存储级别以减少操作
    datapre.count() //进行动作操作来持久化数据，由于还没拿到感染数据，所以不算时间
    //拿到感染数据
    val pretime: Long = System.currentTimeMillis()
    println("----数据预处理完毕----")
    val inf = sc.textFile(infected)
    val infectC = sc.broadcast(inf.collect())//感染者名单
    val cdi = datapre.flatMap(x => { //按基站id遍历
      val infect = infectC.value
      var res: List[String] = List()
      val cnt = Array(0, 0, 0, 0, 0)
      var j = 0
      var k = -1
      for(i<-x.indices){
        k = infect.indexOf(x(i)._1)
        if (k != -1) {
          cnt(k) += x(i)._2
          if(cnt(k)== -1 ){
            for(h<-j to i){
              res = res++List(x(h)._1)
            }
            j = i
            cnt(k) = 0
          }
        } else if(cnt.sum > 0){
          res = res++List(x(i)._1)
        }
      }
      res
    }).distinct()
      .union(inf) //合并，去重
      .sortBy(x=>x,ascending = true,1)
    cdi.saveAsTextFile(output) //保存
    val endtime: Long = System.currentTimeMillis()
    println("红码名单数量:\t", cdi.collect.length)
    println("数据预处理耗时:\t", (pretime - starttime) / 1000, "s")
    println("总计标记红码时长:\t", (endtime - pretime) / 1000, " s")
    println("总计时长:\t", (endtime - starttime) / 1000, " s")
  }
}