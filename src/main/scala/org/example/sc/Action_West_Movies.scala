package org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action_West_Movies {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("wordcount")
      .setMaster("local[*]")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd:RDD[String]=sc.textFile("data\\ml-1m\\movies.dat")

  }
}