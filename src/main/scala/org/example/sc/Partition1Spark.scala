package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Partition1Spark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile("data/word.txt", 5)
    val rdd1:RDD[(Int,String)]=rdd.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x)))
    rdd1.foreach(println(_))
    println("***********")
    val rdd2:RDD[String]=rdd.repartition((2))
    val rdd3:RDD[(Int,String)]=rdd2.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x)))
    rdd3.foreach(println(_))

  }
}
/*
(1,java hadoop xml)
(2,briup java hbase)
(3,hbase java hadoop)
(4,briup java hadoop)
(0,xml java hadoop briup)
***********
(1,java hadoop xml)
(0,xml java hadoop briup)
(0,briup java hbase)
(1,briup java hadoop)
(0,hbase java hadoop)*/