package src.main.scala.org.example.stream

import java.io.PrintWriter
import java.net.ServerSocket

import scala.util.Random

object ProductWord {
  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(8888)
    val arr = Array("hello world", "word good", "test zhansan", "zhansan lisi", "jake lisi", "is a", "apple orange")
    while (true) {
      //阻塞
      val socket = server.accept()
      new Thread() {
        override def run(): Unit = {
          while (true) {
            val num = new Random().nextInt(arr.length)
            val pw = new PrintWriter(socket.getOutputStream)
            pw.println(arr(num))
            pw.flush()
            Thread.sleep(1000)
          }
        }
      }.start()

    }
  }

}
