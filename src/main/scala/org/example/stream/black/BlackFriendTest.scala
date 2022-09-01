package src.main.scala.org.example.stream.black

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}

object BlackFriendTest {
  def blackFriend(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    val blackListInfo = spark.read
      .text("data/blackList.conf")
      .toDF("blackName")
    val info = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .load()
      .as[String]
    val usersDataStream = info.map(line => {
      val Array(id, name) = line.split(" ")
      (id.trim.toInt, name)
    }).toDF("id", "userName").as[(Int, String)];
    val filterDataStream = usersDataStream.join(blackListInfo
      , $"userName" === $"blackName"
      , "left")
      .filter($"blackName".isNull)
    val query1 = filterDataStream.writeStream
      .format("console")
      .start()
    query1.awaitTermination()
  }

  /*
  create table blacklist(
  id int primary key auto_increment,
  blackName varchar(20),
  blackId int
  )
   */
  def blackListCountTest(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql")
      .config("spark.sql.shuffle.partitions", "15")
      .config("spark.default.parallelism", "5")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._
    val blackListInfo = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.72.10:3306/briup?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE&useSSL=false")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "blacklist")
      .load().select($"blackId", $"blackName", row_number()
      .over(Window.partitionBy("blackName")
        .orderBy($"blackId".asc)
      ).as("rn")).where($"rn" === 1)
    val info = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9000)
      .load()
      .as[String]
    val userDataStream:Dataset[(Int,String,Timestamp)]= info.map(line => {
      val Array(id, name, times) = line.split(" ")
      (id.trim.toInt, name, new Timestamp(times.trim.toLong))
    }).toDF("id", "userName", "timestamp")
      .as[(Int, String, Timestamp)]
      .withWatermark("timestamp", "5 seconds")
    val filterDataStream = userDataStream.join(blackListInfo
      , $"userName" === $"blackName"
      , "left")
      .filter($"blackName".isNull)
    import org.apache.spark.sql.functions.window
    val oneMinCountDtream = filterDataStream.groupBy(
      window($"timestamp","1 minutes")
      , $"userName", $"id")
      .count()
      .filter($"count" >= 6)

    val fw = new ForeachWriter[(Int, String)] {
      val url = "jdbc:mysql://192.168.72.10:3306/shop?useUnicode=true&characterEncoding=utf8"
      var offset = 0
      val user = "root"
      val passwd = "root"
      var conn: Connection = null
      var pstmt: PreparedStatement = null
      val sql: String = "insert into blackList(blackName,blackId) values(?,?)"

      override def open(partitionId: Long, version: Long): Boolean = {
        conn = DriverManager.getConnection(url, user, passwd)
        true
      }

      override def process(value: (Int, String)): Unit = {
        println("--------")
        pstmt = conn.prepareStatement(sql)
        pstmt.setString(1, value._2)
        pstmt.setInt(2, value._1)
        pstmt.execute()
      }

      override def close(errorOrNull: Throwable): Unit = {
        if (pstmt != null) pstmt.close()
        if (conn != null) conn.close()
      }
    }
    val query1 = oneMinCountDtream
      .select($"id", $"useName")
      .as[(Int, String)]
      .filter($"userName".isNull)
      .writeStream
      .outputMode(OutputMode.Update())
      .foreach(fw)
      .start()
    query1.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    blackListCountTest()
  }
}