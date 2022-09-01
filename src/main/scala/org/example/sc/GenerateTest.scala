package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GenerateTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    var seq:Seq[(String,Int)]=Seq(("a",1),("a",2),("a",3),("b",2),("b",4))
    val rdd:RDD[(String,Int)]=sc.parallelize(seq,2)
    rdd.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x))).foreach(println)
    println("*************")
    val rdd1:RDD[(String,Iterable[Int])]=rdd.groupByKey()
    rdd1.foreach(println)
    println("-----------")
    //val rdd2:RDD[(String,Int)]=rdd1.map(x=>(x._1,x._2.toList.sum))
    val rdd2=rdd1.mapValues(iter=>iter.reduce((x,y)=>x+y))
    rdd2.foreach(println)
    /*(0,(a,1))
    (1,(a,3))
    (0,(a,2))
    (1,(b,2))
    (1,(b,4))
    *************
    (b,CompactBuffer(2, 4))
    (a,CompactBuffer(1, 2, 3))
    -----------
    (b,6)
    (a,6)
    */
    //val rdd: RDD[Int] = sc.parallelize(1 to 10)
    //val rd1: RDD[String] = rdd.mapPartitions(iter => iter.map(x => x + "_"))
    //rd1.foreach(println)

  }
}
