package src.main.scala.org.example.sql

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, SaveMode, SparkSession}

case class Emp(id:Long,name:String,salary:Double,dept_id:Long)
case class Dept(ids:Long,name:String)
object DsApiTest {
  def fun_test={
    Logger.getLogger("org").setLevel(Level.WARN)
    val  spark:SparkSession=SparkSession.builder()
      .master("local")
      .appName("create_DataFrame")
      .getOrCreate()
    import spark.implicits._
    val empdf:DataFrame=spark.read
      //header数据文件第一行是列的属性名字
      .option("header",true)
      .option("sep",",")
      .option("inferSchema",true)
      .csv("data/emp.txt")
    val emp:Dataset[Emp]=empdf.as[Emp]
    val deptdf:DataFrame=spark.read
      .option("sep",",")
      .option("inferSchema",true)
      .csv("data/dept.txt")
      .toDF("ids","name")
    val dept:Dataset[Dept]=deptdf.as[Dept]
    //获取列类型的数据
    val col:Column=emp.apply("id")
    val col1:Column=emp.col("name")
    val df:DataFrame=emp.select(col,col1)
    df.show()
    //分组需要结合聚合函数使用
//    val ds:RelationalGroupedDataset=emp.groupBy("dept_id")
//    val max:DataFrame=ds.max("salary")
//    val avg:DataFrame=ds.avg("salary")
//    val count:DataFrame=ds.count()
//    val all:DataFrame=max.join(avg,"dept_id")
//      .join(count,"dept_id")
//    all.show()

    import org.apache.spark.sql.functions._
//    val df1:Dataset[Emp]=emp.limit(3)
    //无参数展示所有数据
//    df1.show()
//    val df1:Dataset[Emp]=emp.filter(expr("salary<=3000 and salary>=2500"))
//    df1.show(20)
//    val df:Dataset[Emp]=emp.filter(x=>{
//      val b:Boolean=x.salary<3000 && x.salary>=2500
//      b
//    })
//    df.show(20)
//    val df:Dataset[Emp]=emp.where("dept_id is null")
//    df.show(20)
//    val df:Dataset[Emp]=emp.where("id in(2,3,4)")
//    df.show(20)
//    val df:Dataset[Emp]=emp.where("id>2 and id<5")
//    df.show(20)

//    val df:DataFrame=emp.selectExpr("id","name","salary*12")
//    df.show(20)
//    val df:DataFrame=emp.select(col("id"),$"name",expr("salary*13"))
//    df.show(20)
//    val df:DataFrame=emp.select(col("id"),$"name",$"salary"*13)
//    df.show(20)
//    val df:DataFrame=emp.select("id","name","salary")
//    df.show(20)
//    val df:DataFrame=emp.select("id","name","salary")
//    df.show(20)

//    empdf.printSchema()
    //指定分区数，数据均匀的散落到各个指定分区
//    val df1:Dataset[Emp]=emp.coalesce(3)
//    df1.show(10)
//    val df1:DataFrame=Seq((1,2,3)).toDF("col0","col1","col2")
//    val df2:DataFrame=Seq((4,5,6)).toDF("col1","col2","col0")
    //数据集基于列的名字合并，各个数据集列的位置可以不一致
//    val df:DataFrame=df1.unionByName(df2)
//    df.show(2)
//    val df:DataFrame=df1.union(df2)
//    df.show(2)
    //求并集，要求合并的两个数据集列个数和类型一致
//    val df6:Dataset[Emp]=emp.union(emp)
//    val df:Dataset[Emp]=df6.distinct()
//    df.show(10)
    //局部排序。分区内部排序
//    val df6:Dataset[Emp]=emp.sortWithinPartitions($"salary".desc,$"name".asc)
//    df6.show(10)
//    val df6:Dataset[Emp]=emp.sort(column("salary").desc,column("name").asc)
//    df6.show(10)
//    val df5:Dataset[Emp]=emp.sort(expr("salary").desc,expr("name").asc)
//    df5.show(10)
//    val df4:Dataset[Emp]=emp.sort(col("salary").desc,col("name").asc)
//    df4.show(10)
//    val df3:Dataset[Emp]=emp.sort($"salary".desc)
//    df3.show(10)
//    val df2:Dataset[Emp]=emp.sort($"salary".desc,$"name".asc)
//    df2.show(10)
//    val df1:Dataset[Emp]=emp.sort("salary","name")
//    df1.show(10)
//    val df:Dataset[Emp]=emp.sort("salary")
//    df.show(10)
    //将数据集直接转化为一个新的数据集
//    val df3:Dataset[Emp]=emp.transform(x=>x)
//    df3.show(10)
    //抽样，第一个参数表示抽取数据之后是否放回，第二个参数表示抽样的比例
//    val df3:Dataset[Emp]=emp.sample(true,0.5)
//    df3.show(10)
    //重分区，按照指定的列排序，确定分割点，数据在基于分割点分区
//    val df3:Dataset[Emp]=emp.repartitionByRange(2,$"salary")
//    df3.show(10)
    //重分区，列相同的进入同一个分区
//    val df3:Dataset[Emp]=emp.repartition(2,$"salary")
//    df3.show(10)
    //重分区
//    val df3:Dataset[Emp]=emp.repartition(3)
//    df3.show(10)
//    val df3:Dataset[Emp]=emp.orderBy(expr("salary"),expr("name").desc)
//    df3.show(10)
//    val df3:Dataset[Emp]=emp.orderBy(col("salary"),col("name").desc)
//    df3.show(10)
//    val df2:Dataset[Emp]=emp.orderBy($"salary",$"name".desc)
//    df2.show(10)
    //默认指定升序，直接写属性名
//    val df1:Dataset[Emp]=emp.orderBy("salary","name")
//    df1.show(10)

//    val df:Dataset[Emp]=emp.orderBy("salary")
//    df.show(10)
//    val df:Dataset[(Emp,Dept)]=emp.joinWith(dept,$"dept_id"===$"ids","left")
//    val df:Dataset[(Emp,Dept)]=emp.joinWith(dept,$"dept_id"===$"ids")
//    df.show(10)
    //使用col构建column对象需要导包
    import org.apache.spark.sql.functions._
//    val df:DataFrame=emp.join(dept,col("dept_id").equalTo(col("ids")),"right")
//    df.show(10)
//    val df:DataFrame=emp.join(dept,col("dept_id")===col("ids"),"right")
//    df.show(10)
//    val df:DataFrame=emp.join(dept,$"dept_id"===$"ids","right")
//    df.show(10)
    //连接操作，第二个参数是连接条件
//    val df:DataFrame=emp.join(dept,$"dept_id"===$"ids")
//    df.show(10)
    //连接操作，第二个参数是连接的属性（可以是多个），必须两边数据集都有,第三个参数连接方式
//    val df:DataFrame=emp.join(dept,Array("dept_id"),"left")
//    df.show(10)
    //连接操作，第二个参数是连接的属性，必须两边数据集都有
//    val df:DataFrame=emp.join(dept,"dept_id")
//    df.show(10)
    //连接操作，笛卡尔集
//    val df:DataFrame=emp.join(dept)
//    df.show(10)
//    val ds:Dataset[Emp]=emp.filter(x=>x.salary>3000 )
//    ds.show(10)
//    val ds:Dataset[String]=emp.mapPartitions(iter=>iter.map(x=>x.name))
//    ds.show(10)
//    val ds:Dataset[String]=emp.flatMap(x=>x.name.split("[.]"))
//    ds.show(10)
//    val ds1:Dataset[(String,Double)]=
//      emp.map(x=>(x.name,x.salary))
//    ds1.show(10)

  }
  //start---
  def read_test={
    Logger.getLogger("org").setLevel(Level.WARN)
    val  spark:SparkSession=SparkSession.builder()
      .master("local")
      .appName("create_DataFrame")
      .getOrCreate()
    import spark.implicits._
//    val csv:DataFrame=spark.read
//      .option("sep",";")
//      .csv("data/resources/people.csv")
//    println("csv:")
//    csv.show()
//    val text:DataFrame=spark.read
//      .text("data/resources/people.txt")
//    println("df_text:")
//    text.show()
//    val text1:Dataset[String]=spark.read
//      .textFile("data/resources/people.txt")
//    println("ds_text:")
//    text1.show()
//    val text1:DataFrame=spark.read
//      .json("data/resources/people.json")
//    println("json:")
//    text1.show()
//    val parquet:DataFrame=spark.read
//      .parquet("data/resources/users.parquet")
//    println("parquet:")
//    parquet.show()
//    val orc:DataFrame=spark.read
//      .orc("data/resources/users.orc")
//    println("orc:")
//    orc.show()
    //注意；没有avro方法

//    val avro:DataFrame=spark.read.format("com.databricks.spark.avro")
//      .load("data/resources/users.avro")
//    println("orc:")
//    avro.show()
    //
    //format指明使用什么方式读取数据文件，text、csv、parquet、orc等
//    val csv:DataFrame=spark.read.option("sep",";").format("csv")
//      .load("data/resources/people.csv")
//    println("csv:")
//    val csv:DataFrame=spark.read.option("sep",";").format("csv")
//      .option("inferSchema",true)
//      .load("data/resources/people.csv")
//    println("csv:")
//    csv.show()

    /*val url:String="jdbc:mysql://localhost:3306/shop"
    val table:String="t_user"
    val pro:Properties=new Properties()
    pro.put("user","root")
    pro.put("password","root")
    val m:DataFrame=spark.read.jdbc(url,table,pro)
    m.show(10)*/
//    val emp:DataFrame=spark.read.option("header",true)
//      .option("delimiter","::")
//      .csv("data/student.txt")
//    emp.createOrReplaceTempView("emp")
//    val ed:DataFrame=spark.read.table("emp")
//    ed.show(20)
//    val p:DataFrame=spark.read.format("parquet").load("data/resources/users.parquet")
//    p.show(10)
  }
  def write_test{
    Logger.getLogger("org").setLevel(Level.WARN)
    val  spark:SparkSession=SparkSession.builder()
      .master("local")
      .appName("create_DataFrame")
      .getOrCreate()
    //Row(id,name,salary,dept)
    val df:DataFrame=spark.read.option("header",true)
      .option("inferSchema",true).csv("data/emp.txt")
    import spark.implicits._
    val ds:Dataset[Emp]=df.as[Emp]
    //默认保存parquet文件
    //ds.write.save("data/sf/emp1")
//    df.write.json("data/sf/emp2_json")
    //df.write.csv("data/sf/emp3_csv")
//    df.write.orc("data/sf/emp5_orc")
    //通用模式
//    df.write.format("avro").save("data/sf/emp6_avro")
    df.write.format("orc")
      .option("orc.bloom.filter.columns","id")//设置布隆过滤器
      .option("orc.dictionary.key.threshold","1.0")//版本
      .option("orc.column.encoding.direct","name")//设置编码，字典编码
      .mode(SaveMode.Overwrite)
      .save("data/sf/emp7_orc")
  }
  def sql_read_test: Unit ={
    Logger.getLogger("org").setLevel(Level.WARN)
    val  spark:SparkSession=SparkSession.builder()
      .master("local")
      .appName("create_DataFrame")
      .config("spark.sql.warehourse.dir","spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
//    val df:DataFrame=spark.sql("select * from parquet.`data/resources/users.parquet`")
//    df.show(10)
//    val df:DataFrame=spark.sql("select * from json.`data/resources/employees.json`")
//    df.show(10)
//    val df:DataFrame=spark.sql("select * from csv.`data/resources/people.csv`")
//    df.show(10)
    //spark.sql("create database huzl")
//    val df:DataFrame=spark.sql("show databases")
//    df.show()
//    spark.sql("use huzl")
//    spark.sql("create table stu(id int,name string,age int)")
//    spark.sql("insert into stu values(1,'lisi',30)")
    val df:DataFrame=spark.sql("select * from huzl.stu")
    df.show()
  }

  def main(args: Array[String]): Unit = {
    sql_read_test
  }
}
