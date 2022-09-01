package src.main.scala.org.example.stream


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileSourceTest {
  def json_test= {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[20]")
      .appName("jsontest")
      .getOrCreate()
    import spark.implicits._
    val sch=StructType(Array(
      StructField("name",StringType,false),
        StructField("age",IntegerType,false)
    ))
    //val df: DataFrame = spark.readStream
    //  .schema(sch)
    //  .json("data/json")
    //val query=df.writeStream.format("console")
    //  .outputMode(OutputMode.Append())
    //  .trigger(Trigger.ProcessingTime(0L))
    //  .start()
    //query.awaitTermination()
    val df: DataFrame = spark.readStream
      .format("json")
      .schema(sch)
      .option("path","data/json")
      .load()
    val query=df.writeStream.format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(0L))
      .start()
    query.awaitTermination()
  }
  def text_test= {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[20]")
      .appName("jsontest")
      .getOrCreate()
    val sch = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("salary", DoubleType, false),
      StructField("dept_id", IntegerType, false)
    ))
    val df: DataFrame = spark.readStream
      .format("csv")
      .schema(sch)
      .option("path", "data/csv")
      .option("header", true)
      .load()
    val query = df.writeStream.format("console")
      //  .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(0L))
      .start()
    query.awaitTermination()
  }
  def kafka_test= {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local[20]")
      .appName("jsontest")
      .getOrCreate()
    val sch = StructType(Array(
      StructField("id", LongType, false),
      StructField("name", StringType, false),
      StructField("salary", DoubleType, false),
      StructField("dept_id", IntegerType, false)
    ))
    val df: DataFrame = spark.readStream
      .format("kafka")
      .schema(sch)
      .option("kafka.bootstrap.servers","192.168.72.10:9092")
      .option("subscribe", "spark_test")
      .load()
    df.printSchema()
    val df1:DataFrame=df.selectExpr("cast(value as string","topic","partition")

    val query = df1.writeStream.format("console")
      .trigger(Trigger.ProcessingTime(0L))
      .start()
    query.awaitTermination()
  }
    def main(args: Array[String]): Unit = {
      kafka_test
    }
  }
