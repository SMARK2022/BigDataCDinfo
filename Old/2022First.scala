%Spark平台的Scala代码如下
package Experiment4

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{Row, SparkSession}

object Experiment4 {
    def main(args: Array[String]): Unit = {
        //建立Spark连接
        val spark = SparkSession.builder().appName("Experiment4").getOrCreate()
        val sc = spark.sparkContext
        // 将txt文件按照csv格式读入
        val cdinfo = spark.read.csv(args(0))
        val infected = spark.read.csv(args(1))
        val infected_tlp=infected.select("_c0").rdd.map(row => row(0)).collect.toList.distinct
        // 找出感染的基站
        val filtered = cdinfo.filter(cdinfo("_c3").isin(infected_tlp:_*))
        val infected_base_num_ori=filtered.select("_c0").rdd.map(row => row(0)).collect.toList
        // 基站去重
        var infected_base_num = List[Any]()
        for (i <- 0 to (infected_base_num_ori.length-1)){
            if(i%2==1){
                infected_base_num = infected_base_num :+ infected_base_num_ori(i)
            }
        }
        // 使用两个列表记录基站开始及结束的污染时间
        val infected_base_start = filtered.filter(col("_c2")===1).select("_c1").rdd.map(row => row(0)).collect.toList
        val infected_base_end = filtered.filter(col("_c2")===2).select("_c1").rdd.map(row => row(0)).collect.toList

        val infected_base=infected_base_num.distinct
        val infected_people=cdinfo.filter(cdinfo("_c0").isin(infected_base:_*))
        val may_infected = infected_people.groupBy("_c0","_c3").agg(collect_list("_c1").as("time"))
        may_infected.show(false)

    // 使用UDF（用户自定义函数）过滤
        def is_infected( r:Row ) : Boolean = {
            for (i <- 0 to (infected_base_num.length-1))
            {
                if (r.getAs("_c0")==infected_base_num(i))
                {
                    if (((r.getAs[Seq[String]]("time"))(0).toLong <=  infected_base_end(i).toString.toLong ) && ((r.getAs[Seq[String]]("time"))(1).toLong >= infected_base_start(i).toString.toLong))
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    // 注册用户自定义函数
        spark.udf.register("is_infected",is_infected _)
        val final_infected = may_infected.filter(callUDF("is_infected",struct(may_infected.columns.map(may_infected(_)) : _*)))
        final_infected.show()
        // 将结果写入txt文件
        val basic_task=infected_people.select(("_c3")).distinct().sort("_c3")
        basic_task.coalesce(1).write.text(args(2))
        val final_task= final_infected.select(("_c3")).distinct().sort("_c3")
        final_task.coalesce(1).write.text(args(3))
    }
}
