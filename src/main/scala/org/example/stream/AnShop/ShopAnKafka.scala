package org.example.stream.AnShop

import org.apache.log4j.{Level, Logger}


import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ShopAnKafka {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master(master = "local[*]").appName(name = "ShopAnKafka")
      .config("spark.sql.streaming.checkpointLocation", "data/checkpoint")
      .getOrCreate()
    val df: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "116.62.33.159:9092")
      .option("subscribe", "ShopInfos")
      .load()
    //...
    val query = df.writeStream.format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime(0L))
      .outputMode(OutputMode.Update())
      .start()
    query.awaitTermination()
    spark.close()
  }

}

