package src.main.scala.org.example.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class People(name:String,age:Long)
object JsonSpark {
  def read_json = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]").appName("readJson")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.option("inferSchema",true).json("data/json/people.json")
    val ds:Dataset[People]=df.as[People]

    //df.printSchema()
    //val ds: Dataset[Long] = df.map(row => row.getLong(0))
    df.show(3)
  }

  def main(args: Array[String]): Unit = {
    read_json
  }
}