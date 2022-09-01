package src.main.scala.org.example.shop

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object ShopToKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("shop streaming123")
      .config("spark.sql.streaming.checkpointLocation", "spark-checkpoint")
      .config("spark.sql.shuffle.partitions", "15")
      .config("spark.default.parallelism", "5")
      .getOrCreate()
    import spark.implicits._

    //监控数据目录
    val initData: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.72.10:9092")
      .option("subscribe", "shopInfos")
      .load()
      .selectExpr("cast(value as string) value")
      .as[String]

    //处理数据
    val formatData = initData.map(line => {
      val Array(id, count, time, isLike, buyCount, timestamp) = line.split("::")
      ShopInfo(id, count.trim.toInt, time.trim.toDouble, isLike.trim.toInt, buyCount.trim.toInt, new Timestamp(timestamp.toLong))
    })

    //计算每个商品关注度
    val shopAndWigthData: Dataset[ResultInitData] = formatData.map(shopInfo => {
      val wigth = shopInfo.count * 0.8 + shopInfo.time * 0.6 + shopInfo.isLike + shopInfo.buyCount
      (shopInfo.id, wigth, shopInfo.timestamp)
    }).toDF("id", "wigth", "timestamp").as[ResultInitData]

    //计算每个商品的实时关注度以及连续五分钟不间断的关注度，并保存到Kafka集群中
    val mapGroup = (shopId: String, iter: Iterator[ResultInitData], state: GroupState[ResultState]) => {
      //将shopid商品所对应的全部数据转换成list集合
      val list = iter.toList
      //先判断state是否过时
      if (state.hasTimedOut) {
        //过时了，获取最终资源，并移除数据
        val resultState = state.get
        ResultData(shopId, resultState.nowData, resultState.avgData, resultState.window, flag = true)
      } else {
        //判断state是否存在
        val newState = if (state.exists) {
          //已经存在，更新状态
          val oldState = state.get
          //获取最大时间对应关注度
          val nowData = list.maxBy(_.timestamp.getTime).wigth
          //获取这批商品最大时间
          val window = list.maxBy(_.timestamp.getTime).timestamp
          //获取这批商品平均关注度
          val avgData = (list.map(_.wigth).sum / list.size + oldState.avgData) / 2
          new ResultState(nowData, avgData, window, flag = true)
        } else {
          //第一次出现
          //获取这批商品最大时间对应关注度
          val nowData = list.maxBy(_.timestamp.getTime).wigth
          //获取这批商品最大时间
          val window = list.maxBy(_.timestamp.getTime).timestamp
          //获取这批商品平均关注度
          val avgData = list.map(_.wigth).sum / list.size
          ResultState(nowData, avgData, window, flag = true)
        }
        //更新状态对象
        state.update(newState)
        //设置超过时间
        state.setTimeoutDuration("5 minute")
        //返回数据
        ResultData(shopId, newState.nowData, newState.avgData, newState.window, flag = true)
      }
    }
    val result = shopAndWigthData.groupByKey(_.id)
      .mapGroupsWithState[ResultState, ResultData](GroupStateTimeout.ProcessingTimeTimeout())(mapGroup)

    val query = result
      .selectExpr("cast(concat('{\"window\":\"',cast(window as String),'\",\"key\":\"',id,'\"}')" +
        "as String) as key", "cast(concat('{\"window\":\"',cast(window as String),'\",\"key\":\"'," +
        "cast(id as String),'\",\"avg\":\"',cast(avg as String),'\",\"now\":\"',cast(avg as String),'\"}')" +
        "as String) as value")
      .writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      //        .format("console")
      //        .option("truncate",false)
      .option("kafka.bootstrap.servers", "192.168.72.10:9092")
      .option("kafka.acks", "0")
      //计算之后的数据写入kafka的任意主题，主题就是web用户名，（基于主题名字注册用户）
      .option("topic", "tom")

      .start()
    query.awaitTermination()
    spark.close()
  }
}