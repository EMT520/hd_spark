package src.main.scala.org.example.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Dog(id:Long,name:String,age:Int)
object DFCreate {
  def create_DataFrame = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("create_DataFrame")
      .getOrCreate()
    val sc:SparkContext=spark.sparkContext
    val seq: Seq[Dog] = Seq(
      new Dog(1, "xiaohei", 2),
      new Dog(2, "xiaobai", 3),
      new Dog(3, "lulu", 1)
    )
    val rdd:RDD[Dog]=sc.parallelize(seq)
    import spark.implicits._
    val df:DataFrame=rdd.toDF("id","name","age")
    val df1:DataFrame=seq.toDF()
    val ds:Dataset[Dog]=df.as[Dog]
    ds.show(10)
    val ds1:Dataset[Dog]=seq.toDS()
    ds1.show(10)
    val ds2:Dataset[Dog]=rdd.toDS()
    ds2.show(10)

  }

  def main(args: Array[String]): Unit = {
    create_DataFrame
  }
}