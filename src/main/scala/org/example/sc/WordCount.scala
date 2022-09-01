package org.example.sc



import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf=new SparkConf()
      .setAppName("wordcount")
      .setMaster("local[8]")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc:SparkContext=SparkContext.getOrCreate(conf)
    val rdd:RDD[String]=sc.textFile("data/word.txt",10)
    println(rdd.getNumPartitions)
    rdd.foreach(println)
    sc.stop()
  }

}
