package src.main.scala.org.example.sc

import java.io.{DataInput, DataOutput}

import org.apache.avro.file.BZip2Codec
import org.apache.hadoop.io.{IntWritable, Text, Writable}
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SeqSpark {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[*]").setAppName("seqspark")
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc=SparkContext.getOrCreate(conf)
    val rdd:RDD[String]=sc.textFile("data/ml-1m/movies.dat")
    val srdd:RDD[(String,Int)]=rdd.map(x=>(x,x.length))
    val bigrdd:RDD[(Text,IntWritable)]=srdd.map(x=>(new Text(x._1),new IntWritable(x._2)))
    bigrdd.saveAsNewAPIHadoopFile[TextOutputFormat[Text,IntWritable]]("data/h_result")
    //srdd.saveAsSequenceFile("dara/seq")
    //rdd.saveAsObjectFile("data/result")
    //val sordd:RDD[String]=sc.objectFile[String]("data/result")
    //srdd.saveAsSequenceFile("data/seq",Option(classOf[BZip2Codec]))
    //srdd.saveAsSequenceFile("data/seq2",Some(classOf[BZip2Codec]))

    val rs:RDD[(String,Int)]=sc.sequenceFile[String,Int]("data/seq2")
    rs.foreach(println)


    //key->String value->Int
    /*val list:List[(String,Int)]=List(
      ("a",1),
      ("b",2),
      ("c",3),
      ("b",4),
      ("e",5),
      ("f",6)
    )
    val rdd:RDD[(String,Int)]=sc.parallelize(list,1)
    //saveAsSequenceFile 要求数据必须是键值对
    rdd.saveAsSequenceFile("data/seq1")
*/
    //key->Tea value->Int
   /* val seq:Seq[(Tea,Int)]=Seq(
      (new Tea(1,"lisi",30),1),
      (new Tea(2,"tom",31),2),
      (new Tea(3,"jake",23),2),
      (new Tea(4,"rose",34),3),
      (new Tea(5,"lili",19),4),
    )
    val rdd:RDD[(Tea,Int)]=sc.makeRDD(seq,1)
    rdd.saveAsSequenceFile("data/seq2")
    sc.stop()*/
  }

}
/*
case class Tea(var id:Long,var name:String,var age:Int)
extends Serializable with  Writable{
  override def write(out:DataOutput):Unit={
    out.writeLong(id)
    out.writeUTF(name)
    out.writeInt(age)
  }
  override def readFields(in:DataInput):Unit={
    this.id=in.readLong()
    this.name=in.readUTF()
    this.age=in.readInt()
  }
}
*/