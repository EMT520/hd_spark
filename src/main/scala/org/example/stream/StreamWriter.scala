package src.main.scala.org.example.stream


import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.concurrent.TimeUnit
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}

object StreamWriter {
  def json_write_test = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("json")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 8888)
      .option("includeTimestamp", true)
      .load()
    val ds: Dataset[(String, Int)] = df.flatMap(row =>
      row.getString(0).split(" ").map(x => (x, 1)))
    val df1 = ds.toDF("word", "num")
    val query = df1.writeStream.format("json")
      .option("path","data/dataS_json")
      .trigger(Trigger.ProcessingTime(0L))
      .start()
    query.awaitTermination()

  }
  def parquet_write_test = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("json")
      .config("spark.sql.streaming.checkpointLocation","data/checkpoint")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 8888)
      .load()
    val ds: Dataset[(String, Int)] = df.flatMap(row =>
      row.getString(0).split(" ").map(x => (x, 1)))
    val df1 = ds.toDF("word", "num")
    val query = df1.writeStream.format("parquet")
      .option("path","data/dataS_parquet")
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()

  }
  def read_mysql_test = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("json")
      .config("spark.sql.streaming.checkpointLocation","data/checkpoint")
      .getOrCreate()
    val df: DataFrame = spark.readStream.format("jdbc")
      .option("url", "jdbc:mysql://192.168.72.10:3306/shop?useUnicode=true&characterEncoding=utf8")
      .option("user","root")
      .option("password", "root")
      .option("dbtable","t_user")
      .load()
    val query = df.writeStream.format("console")
      .trigger(Trigger.ProcessingTime(0L))
      .outputMode(OutputMode.Append())
      .start()
    query.awaitTermination()

  }
  def writer_mysql_test = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("w_m")
      .config("spark.sql.streaming.checkpointLocation", "data/checkpoint")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 8888)
      .load()
    val ds: Dataset[(String, Int)] = df.flatMap(row =>
      row.getString(0).split(" ").map(x => (x, 1)))
    val fw=new ForeachWriter[(String, Int)] {
      val url="jdbc:mysql://192.168.72.10:3306/shop?useUnicode=true&characterEncoding=utf8"
      val user="root"
      val passwd="root"
      var con: Connection = null
      var ps:PreparedStatement=null
      val sql:String="insert into t_word(id,word) values(?,?)"

      override def open(partitionId: Long, epochId: Long): Boolean = {
        con=DriverManager.getConnection(url,user,passwd)
        true
      }

      override def process(value: (String, Int)): Unit = {
        ps=con.prepareStatement(sql)
        ps.setInt(1,value._2)
        ps.setString(2,value._1)
        ps.execute()
      }

      override def close(errorOrNull: Throwable): Unit = {
        if(ps!=null)ps.close()
        if(con!=null)con.close()
      }
    }
    val query = ds.writeStream.foreach(fw)
      .outputMode(OutputMode.Complete())
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .start()
    query.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    writer_mysql_test
  }
}