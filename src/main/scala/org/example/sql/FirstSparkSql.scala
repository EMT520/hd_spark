package src.main.scala.org.example.sql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object FirstSparkSql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkSession: SparkSession = SparkSession.builder()
      .master("local").appName("FirstSparkSql")
      .getOrCreate()
    //val df: DataFrame = sparkSession.read.text("data/word.txt")
    //df.show(10)
    val df:DataFrame=sparkSession.read.csv("data/student.txt")
    df.show(20,true)

  }
}