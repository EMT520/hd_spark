package src.main.scala.org.example.stream

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WindowTest {
  def window_wordcount = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[10]")
      .appName("window_wordcount")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "192.168.72.10")
      .option("port", 8888)
      .option("includeTimestamp", true)
      .load()
    //    val ds:Dataset[(String,Int,Timestamp)]=df.flatMap(row=>{
    //      var arr:Array[String]=row.getString(0).split(" ")
    //      var arr1:Array[(String,Int,Timestamp)]=arr.map(x=>(x,1,row.getTimestamp(1)))
    //      arr1
    //    })
    val ds: Dataset[(String, Int, Timestamp)] = df.flatMap(row => {
      row.getString(0).split(" ").map(x => (x, 1, row.getTimestamp(1)))
    })
    val df1=ds.toDF("word","num","time")
    import org.apache.spark.sql.functions._
    val df2:DataFrame=df1.groupBy(
      window($"time","60 seconds","60 seconds"),$"word")
      .agg("num"->"sum")
    val query = df2.writeStream.format("console")
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    window_wordcount
  }
}