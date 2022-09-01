package src.main.scala.org.example.shop

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
case class ShopInfo(id:String,count:Int,time:Double,isLike:Int,buyCount:Int,timestamp:Timestamp)
object ShopStreamingSelfTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("shop streaming")
    //  .config("spark.sql.streaming.checkpointLocation","data/checkpoint")
      .getOrCreate()
    import spark.implicits._

    val initData=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.72.10:9092")
      .option("subscribe", "shopInfos")
      .load()
      .selectExpr("cast(value as string) value")
      .as[String]
    //处理数据
    val formatData=initData.map(line=>{
      val Array(id,count,time,isLike,buyCount,timestamp)=line.split("::")
      ShopInfo(id,count.trim.toInt,time.trim.toDouble,isLike.trim.toInt,buyCount.trim.toInt,new Timestamp(timestamp.toLong))
    })
    //计算每个商品关注度
    val shopAndWigthData=formatData.map(shopInfo=>{
      val wigth=shopInfo.count*0.8+shopInfo.time*0.6+shopInfo.isLike+shopInfo.buyCount
      (shopInfo.id,wigth,shopInfo.timestamp)
    }).toDF("id","wigth","timestamp")
    //计算两分钟内的平均期货关注度
    import org.apache.spark.sql.functions._
    val query1=shopAndWigthData
      .withWatermark("timestamp","1 minute")
      .groupBy(window($"timestamp","2 minute","1 minute"),$"id")
      .avg("wigth")
      .sort($"window".asc,$"id".desc)
      .toDF("window","id","avg")
      .selectExpr("cast(concat('{\"window\":\"',cast(window as String),'\",\"key\":\"',id,'\"}')"+
        "as String) as key","cast(concat('{\"window\":\"',cast(window as String),'\",\"key\":\"',"+
      "cast(id as String),'\",\"avg\":\"',cast(avg as String),'\",\"now\":\"',cast(avg as String),'\"}')"+
      "as String) as value")
        .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.72.10:9092")
      .option("topic", "zs")
      .option("checkpointLocation","stream-kafka-checkpoint")
      .outputMode(OutputMode.Append())
      .start()
    query1.awaitTermination()
    spark.close()


  }

}
