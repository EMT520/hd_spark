#!/usr/bin/env scala
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.util.{Calendar, Date}

class A(a:String){

  val user_agent_dist=mutable.Map.empty[Double,String];
  val ip_slice_list=ArrayBuffer.empty[Int];
  val url_path_list=ArrayBuffer.empty[String];
  val http_refer=ArrayBuffer.empty[String];
  val search_keyword=ArrayBuffer.empty[String];

  def this(){
    this("");
    //前面7条是IE,所以大概浏览器类型70%为IE ，接入类型上，20%为移动设备，分别是7和8条,5% 为空
    //https://github.com/mssola/user_agent/blob/master/all_test.go
    user_agent_dist ++= mutable.Map(0.0->"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
      0.1->"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
    0.2->"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)",
    0.3->"Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
    0.4->"Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
    0.5->"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
    0.6->"Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
    0.7->"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53",
    0.8->"Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19",
    0.9->"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
	1.0->" "
	)
    //随机ip生成列表
    ip_slice_list ++= ArrayBuffer(10, 29, 30, 46, 55, 63, 72, 87, 98,132,156,124,167,143,187,168,190,201,202,214,215,222)
    //url路径列表
    url_path_list ++= ArrayBuffer("login.php","view.php","list.php","upload.php","admin/login.php","edit.php","index.html")
    //http协议
    http_refer ++= ArrayBuffer("http://www.baidu.com/s?wd={query}","http://www.google.cn/search?q={query}","http://www.sogou.com/web?query={query}","http://www.yahoo.com/s?p={query}","http://cn.bing.com/search?q={query}")
    //搜索关键字
    search_keyword ++= ArrayBuffer("spark","hadoop","hive","spark mlib","spark sql")
  }

  //产生日志
  def sample_one_log(num:Int)={
    //文件数据个数
    var count = num
    //当前时间
    val now: Date = new Date()
    //格式化日期
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time_str = dateFormat.format(now)
    //循环num次
    while(count >1){
      val query_log = s"""$sample_ip - - [$time_str] \"GET /$sample_url HTTP/1.1\" 200 0 \"$sample_refer\" \"$sample_user_agent\" \"-\""""
      //按照格式字符串进行输出
      println(query_log)
      count = count -1
    }
  }

  //主要搜索引擎referrer参数
  def sample_refer()={
    //随机产生一个double类型的百分数
    val key=new Random(1).nextDouble().toString.substring(0,2).toDouble;
    //只有20% 流量有refer
    if (key > 0.2)
      "-"
    //协议字符串
    val refer_str=randomFromIterable(http_refer,1).mkString
    //检索字符串
    val query_str=randomFromIterable(search_keyword,1).mkString
    refer_str.replace("{query}",query_str)
  }

  //随机生成agent
  def sample_user_agent()={
    val key=new Random().nextDouble().toString.substring(0,3).toDouble;
    user_agent_dist.getOrElse(key," ")
  }

  //随机生成URL
  def sample_url()={
    randomFromIterable(url_path_list,1).mkString
  }

  //随机生成IP
  def sample_ip()={
    //从ip_slice_list中随机获取4个元素，作为一个片断返回
    val slice=randomFromIterable(ip_slice_list, 4)
    slice.mkString(".")
  }

  //随机从iter中获取一个值
  def randomFromIterable(iter:Seq[Any],num:Int)={
    for(i <- 1 to num) yield{ iter(Random.nextInt(iter.length-1)) }
  }


}
object A{
  //类属性，由所有类的对象共享
  val site_url_base = "http://www.xxx.com/"
  def main(args: Array[String]): Unit = {
    val a=new A();
    //文件中数据的条数
    val random=new Random().nextInt(90)+10;
    //调用生成日志方法
    a.sample_one_log(random)
  }
}
