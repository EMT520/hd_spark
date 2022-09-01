package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object UserGenderNum {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile("data/ml-1m/users.dat")
    val g_rdd:RDD[(String,Int)]=rdd.map(x=>(x.split("::")(1),1))
    //g_rdd.dependencies.foreach(println)
    val gg_rdd:RDD[(String,Iterable[Int])]=g_rdd.groupByKey()
    //gg_rdd.dependencies.foreach(println)
    val res_rdd:RDD[(String,Int)]=gg_rdd.mapValues(iter=>iter.reduce((x,y)=>x+y))
    val single_RDD:RDD[(String,Int)]=res_rdd.repartition(1)
    single_RDD.foreach(println)
    //single_RDD.saveAsTextFile("/data/us_movie")
    sc.stop()
  }
}
