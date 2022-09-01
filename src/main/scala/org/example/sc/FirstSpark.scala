package org.example.sc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FirstSpark {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf=new SparkConf().setMaster("local").setAppName("firstapp")
    val sc:SparkContext=SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")
    val seq:Seq[Int]=Range.apply(0,20)
    val rdd:RDD[Int]=sc.parallelize(seq)
    val mrdd:RDD[(Int,Int)]=rdd.map(x=>(x,1))
    mrdd.foreach(println)
  }

}
