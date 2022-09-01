package src.main.scala.org.example.sc

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DefinePartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]").setAppName("SeqSpark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc = SparkContext.getOrCreate(conf)
    val seq: Seq[Cus] = Seq(
      new Cus(1,"lsii",30),
      new Cus(2,"tom",23),
      new Cus(3,"jake",35),
      new Cus(4,"lili",32)
    )
    var rdd:RDD[Cus]=sc.parallelize(seq)
    var srdd:RDD[(Cus,Int)]=rdd.map(x=>(x,1))
    var srdd1:RDD[(Cus,Int)]=srdd.partitionBy(new agePartitioner(2))
    srdd1.mapPartitionsWithIndex((index,iter)=>iter.map(x=>(index,x))).foreach(println)
  }
}
case class Cus(var id:Long,var name:String,var age:Int)
class agePartitioner(numPartition:Int) extends Partitioner {
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = key match {
    case cus:Cus=>cus.age%numPartition
    case _=>0
  }
}

