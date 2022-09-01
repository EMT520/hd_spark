package src.main.scala.org.example.stream

import scala.util.Random

object ip {
  def num2ip(ip: Int): String = {
    val b = new Array[Int](4)
    var x = ""
    b(0) = ((ip >> 24) & 0xff).toInt
    b(1) = ((ip >> 16) & 0xff).toInt
    b(2) = ((ip >> 8) & 0xff).toInt
    b(3) = (ip & 0xff).toInt
    x = Integer.toString(b(0)) + "." + Integer.toString(b(1)) + "." + Integer.toString(b(2)) + "." + Integer.toString(b(3))
    x
  }

  def getRandomIp: String = { // ip范围
    val range = Array(Array(607649792, 608174079), // 36.56.0.0-36.63.255.255
      Array(1038614528, 1039007743), // 61.232.0.0-61.237.255.255
      Array(1783627776, 1784676351), // 106.80.0.0-106.95.255.255
      Array(2035023872, 2035154943), // 121.76.0.0-121.77.255.255
      Array(2078801920, 2079064063), // 123.232.0.0-123.235.255.255
      Array(-1950089216, -1948778497), // 139.196.0.0-139.215.255.255
      Array(-1425539072, -1425014785), // 171.8.0.0-171.15.255.255
      Array(-1236271104, -1235419137), // 182.80.0.0-182.92.255.255
      Array(-770113536, -768606209), // 210.25.0.0-210.47.255.255
      Array(-569376768, -564133889)) // 222.16.0.0-222.95.255.255

    val rdint = new Random()
    val index = rdint.nextInt(10)
    val ip = num2ip(range(index)(0) + new Random().nextInt(range(index)(1) - range(index)(0)))
    ip
  }

  def main(args: Array[String]): Unit = {
    println(getRandomIp)
  }
}
