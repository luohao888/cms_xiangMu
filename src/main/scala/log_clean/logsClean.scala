package log_clean

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object logsClean {
  def main(args: Array[String]): Unit = {

    // 2 创建sparkconf->sparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val lines: RDD[String] = sc.textFile(args(0))
    val startlines = lines.filter(s => {
      logsUtils.start(s)
    })
   // startlines.collect().foreach(println)
   println("---------------------------------------------------")


    val eventlines = lines.filter(s => {
     logsUtils.event(s)
    })
    //eventlines.collect().foreach(println)
 startlines.saveAsTextFile(args(1))
    eventlines.saveAsTextFile(args(2))


    sc.stop()




  }
}
