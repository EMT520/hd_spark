package src.main.scala.org.example.AnShop

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ShopAnKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ShopAnKafka")
      .config("spark.sql.streaming.checkpointLocation","data/checkpoint")
      .getOrCreate()
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.72.10:9092")
      .option("subscribe", "shopInfos")
      .load()

    val query = df.writeStream.format("kafka")
      .trigger(Trigger.ProcessingTime(0L))
      .outputMode(OutputMode.Update())
      .start()
    query.awaitTermination()
    spark.close()
  }

}
