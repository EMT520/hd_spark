package src.main.scala.org.example.stream.black

import java.io.PrintWriter
import java.net.ServerSocket

object ProductData {
  def index(length:Int)={
    import java.util.Random
    val rdm=new Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    println("黑名单数据模拟器启动！！！")
    val seq=Seq("001 红娃","002 橙娃","003 黄娃",
      "004 绿娃","005 青娃","006 蓝娃",
      "006 蓝娃","006 蓝娃","005 青娃",
      "006 蓝娃","006 蓝娃",
      "006 蓝娃","006 蓝娃",
      "006 蓝娃","006 蓝娃",
      "006 蓝娃","006 蓝娃",
      "007 紫娃","007 紫娃","007 紫娃",
      "007 紫娃","007 紫娃","007 紫娃",
      "007 紫娃","007 紫娃","007 紫娃",
      "007 紫娃","007 紫娃","007 紫娃",
      "007 紫娃")
    val seqSize=seq.size
    val serversocket=new ServerSocket(9000)
    while (true) {
      //阻塞
      val socket = serversocket.accept()
      new Thread() {
        override def run(): Unit = {
          println("Got client connected from:"+socket.getInetAddress)
          val out =new PrintWriter(socket.getOutputStream(),true)
          while (true) {
            Thread.sleep(1000)
            val content=seq(index(seqSize))
            val times =new java.util.Date().getTime;
            out.write(content+" "+times+'\n')
            println(content)
            out.flush()
          }
          socket.close()
          }
        }.start()
      }
  }

}
