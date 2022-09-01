package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val rdd: RDD[String] = sc.textFile("data/ml-1m/users.dat",4)
    println("rdd:"+rdd.getNumPartitions)

    val gen_rdd:RDD[String]=rdd.map(x=>x.split("::")(1))
    println("gen:"+gen_rdd.getNumPartitions)

    val  g_rdd:RDD[(String,Int)]=gen_rdd.map(x=>(x,1))
    println("g_rdd:"+g_rdd.getNumPartitions)

    val srdd:RDD[(String,Int)]=g_rdd.reduceByKey(_+_)
    println("srdd:"+srdd.getNumPartitions)
    srdd.foreach(println(_))

}
}
