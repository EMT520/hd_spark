package org.example.sc

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRead {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("wordcount")
      .setMaster("local[*]")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    //textFile可以读取本地文件也可以读取分布式文件系统中的内容
    //val rdd: RDD[String] = sc.textFile("hdfs://192.168.72.10:9000/user/hdfs/course.txt")
    //rdd.foreach(println)
    //sc.stop()
    //产生SequenceFile格式的文件k->int v->user
    val seq: List[(Int, User)] = List(
      (1, new User("jake", 30)),
      (2, new User("tom", 23)),
      (3, new User("rose", 20))
    )
      val rdd:RDD[(Int,User)]=sc.parallelize(seq)
    rdd.saveAsSequenceFile("data/seq1.txt")
    sc.stop()

  }
  case class User(var name:String,var age:Int) extends Serializable with Writable {
    override def write(out:DataOutput): Unit = {
      out.writeUTF(name)
      out.writeInt(age)
    }
    override def readFields(in:DataInput): Unit = {
      this.name=in.readUTF()
      this.age=in.readInt()
    }
  }
}
