package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PairRDDSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile("data/word.txt", 4)
    val rd:RDD[(String,Int)]=rdd.flatMap(x=>x.split(" ")).map(x=>(x,1))
    val rd1:RDD[(Int,(String,Int))]=rd.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x)))

    rd1.foreach(println(_))
    println("**********")
    val par=new HashPartitioner(2)
    val rdd2:RDD[(String,Int)]=rd.repartitionAndSortWithinPartitions(par)
    val rd3:RDD[(Int,(String,Int))]=rdd2.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x)))
    rd3.foreach(println(_))
  }
}