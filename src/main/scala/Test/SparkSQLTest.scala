package Test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparksqslTest").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkSession.implicits._
    val df: DataFrame = sparkSession.sparkContext.makeRDD(Array(Score("a1", 1, 60),
      Score("a4", 2, 60),
      Score("a2", 2, 70),
      Score("a3", 3, 90)
    )).toDF("name", "class", "score")
    df.createTempView("scoreTable")
  sparkSession.sql("select name,class,score,count(name) over(partition by class) count_name from scoreTable")

sparkSession.stop()
  }


}
case class Score(name:String,clas:Int,score:Int)

