package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object DefinePartition1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val seq: Seq[(Int,Int)] = Seq(
      (1999,23),
      (1998,28),
      (1992,24),
      (1995,26),
      (1996,16),
      (1993,19),
      (1997,56),
      (1991,31),
      (1994,27)
    )
    var rdd:RDD[(Int,Int)]=sc.parallelize(seq)
    var rdd1:RDD[(Int,Int)]=rdd.partitionBy(
      new RangePartitioner(3,rdd,false))
      rdd1.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x))).saveAsTextFile("data/s_1")
  }
}