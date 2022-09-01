package src.main.scala.org.example.stream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Data(word:String,num:Int)
object WordCountStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("wordcountstream")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()
    val query=df.flatMap(row => row.getString(0).split(" "))
      .map(x => Data(x, 1))
      .toDF("word","num")
      .groupBy("word")
      .agg("num" -> "sum")
      .writeStream.format("console")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(0L))
      .start()
    query.awaitTermination()
  }
}