package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action_Wes_Movies {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile("data/ml-1m/movies.dat")

    //Action
    val action_rdd:RDD[String]=rdd.filter(x=>x.contains("Action"))
    //Western
    val Western_rdd:RDD[String]=rdd.filter(x=>x.contains("Western"))
    val all_rdd:RDD[String]=action_rdd.union(Western_rdd)
    println("数据总行："+ all_rdd.count())
    all_rdd.take(10).foreach(println)
    Thread.sleep(1000000)
  }
}
