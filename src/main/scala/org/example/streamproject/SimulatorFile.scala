package src.main.scala.org.example.streamproject


import java.io._
import scala.io.Source
import scala.util.Random
object SimulatorFile{
  def main(args: Array[String]): Unit = {
    val list=Source.fromFile("conf.properties").getLines().toList;
    val map=list.filter(_.contains("=")).map(line=>line.split("[=]")).map(arr=>{(arr(0),arr(1))}).toMap;
    val MSG_NUM = map.getOrElse("MSG_NUM","30").toInt;
    val BROWSE_NUM = map.getOrElse("BROWSE_NUM","5").toInt;
    val STAY_TIME = map.getOrElse("STAY_TIME","10").toInt;
    val COLLECTION= map.get("COLLECTION") match {
      case Some(line) => line.split(",").toArray
      case None => Array[Int](-1, 0, 1)
    }
    val BUY_NUM=map.get("BUY_NUM") match {
      case Some(line) => line.split(",").toArray
      case None => Array[Int](0, 1, 0, 2, 0, 0, 0, 1, 0)
    }
    val productList = map.get("product").get.split(",").toList;
    try {val r = new Random();
      val msgNum = r.nextInt(MSG_NUM) + 1
      for (i <- 0 to msgNum) {
        val sb = new StringBuffer
        val index=r.nextInt(productList.size);
        sb.append(productList(index))
        sb.append("::")
        sb.append(r.nextInt(BROWSE_NUM) + 1)
        sb.append("::")
        sb.append(r.nextInt(STAY_TIME) + r.nextFloat)
        sb.append("::")
        sb.append(COLLECTION(r.nextInt(2)))
        sb.append("::")
        sb.append(BUY_NUM(r.nextInt(9)))
        sb.append("::")
        sb.append(new java.util.Date().getTime)
        println(sb.toString());
      }
    }catch{
      case e:Exception => e.printStackTrace();
    }
  }
}