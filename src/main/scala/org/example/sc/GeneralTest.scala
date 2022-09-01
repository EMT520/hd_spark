package org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GeneralTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("wordcount")
      .setMaster("local[*]")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    //textFile可以读取本地文件，也可以读取分布式文件系统的内容
    val rdd:RDD[Int]=sc.parallelize(1 to 10)
    val rdd1:RDD[String]=rdd.mapPartitions(iter=>iter.map(x=>x+"_"))

    var seq:Seq[(String,Int)]=Seq(("a",1),("a",2),("a",3))
    val rdd2:RDD[(String,Int)]=sc.parallelize(seq,2)
    rdd2.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x))).foreach(println)
    println("***")
    val rdd4:RDD[(String,Iterable[Int])]=rdd2.groupByKey()
    rdd4.foreach(println)
    println("-----")
    val rdd5:RDD[(String,Int)]=rdd4.map(x=>(x._1,x._2.toList.sum))

    val rdd3=rdd2.combineByKey((x:Int)=>x+"_",(a:String,b:Int)=>a+"$"+b,(x:String,y:String)=>x+"@"+y)
    rdd3.foreach(println)
    rdd3.dependencies.foreach(println)

   /*
   (0,(a,1))->(a,1_)
                          1_@2_$3
    (1,(a,2)) ->(a,2_)
    (1,(a,3))->(a,2_$3)
    */
    //初始值针对每个分区的数据参与一次计算

    var seq1:Seq[(String,Int)]=Seq(("a",1),("b",1),("c",1))
    var seq2:Seq[(String,Int)]=Seq(("a",2),("a",3),("b",2),("d",1))
    val rdd6:RDD[(String,Int)]=sc.parallelize(seq1,2)
    val rdd7:RDD[(String,Int)]=sc.parallelize(seq2,3)

    val m:collection.Map[String,Int]=rdd7.collectAsMap()
    m.foreach(println)
    println("---")
    //键相同的值累加
    val m1:collection.Map[String,Long]=rdd7.countByKey()
    m1.foreach(println)

  }
}