package src.main.scala.org.example.stream.state

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.junit.Test
/**
  * 初始化数据对象
  * @param line
  *             原始数据信息
  * @param time
  *             事件事件
  * */
case class InitData(line:String,time:Timestamp)
/**
  * 格式化之后的数据对象
  * @param word
  *             关键字单词
  * @param time
  *             事件事件
  * */
case class FormatData(word:String,time:Timestamp)
/**
  * 结果数据的状态对象
  * @param word
  *             关键字单词
  * @param count
  *             出现的次数
  * @param firstTime
  *             关键字第一次出现的时间
  * @param beforeTime
  *             上一个关键字出现的时间
  * */
case class ResultState(word:String,count:Long,firstTime:Timestamp,beforeTime:Timestamp)
/**
  * 结果数据对象
  * @param word
  *             关键字单词
  * @param count
  *             出现的次数
  * @param min
  *             关键字第一次出现的时间
  * @param beforeTime
  *             上一个关键字出现的时间
  * @param max
  *             事件发生的最大事件(当前事件时间)
  * */
case class ResultData(word:String,count:Long,min:Timestamp,beforeTime:Timestamp,max:Timestamp)
class MapGroupWithState {
  Logger.getLogger("org").setLevel(Level.WARN)
  //  第一步：获取SparkSession对象
  val spark=SparkSession.builder()
    .master("local[*]")
    .appName("MapGroupWithState")
    .getOrCreate();
  import  spark.implicits._;
  //  第二步：从网络中获取流式的Datatset对象
  val dataset=spark.readStream.format("socket")
    .option("host","localhost")
    .option("port",9999)
    .option("includeTimestamp",true)
    .load()
    .toDF("line","time")
    .as[InitData]
  //  第三步：处理数据
  val formatData=dataset.flatMap(initDate=>{
    initDate.line.split(" ").map(word=>(word,initDate.time))
  }).toDF("word","time").as[FormatData];

  /**
    * mapGroupsWithState操作
    * */
  @Test
  def mapGroupState(): Unit = {
    val mapGroupState=(word:String,iter:Iterator[FormatData],state:GroupState[ResultState])=>{
      //由于Iterator集合的特点，此处将Iterator集合转化为list集合
      val list=iter.toList;
      val resultData=if(state.exists){
        //不是第一个出现
        if(state.hasTimedOut){
          state.remove();
          ResultData(word,0,null,null,null);
        }else{
          //时间未到，合并状态
          val oldState=state.get;
          val count=oldState.count+list.size
          val firstTime=oldState.firstTime;
          val beforeTime=oldState.beforeTime;
          val max=list.head.time;
          //更新状态
          val newState=ResultState(word,count,firstTime,max);
          state.update(newState);
          //设置超时时间
          state.setTimeoutDuration("30 seconds")
          ResultData(word,count,firstTime,beforeTime,max);
        }
      }else{
        //第一次出现,创建新ResultData对象
        //获取单词出现的次数
        val count=list.size;
        //获取事件时间 第一次min=beforeTime=max
        val eventTime=list.head.time;
        //更新状态
        val newState=ResultState(word,count,eventTime,eventTime);
        state.update(newState);
        //设置超时时间
        state.setTimeoutDuration("30 seconds")
        ResultData(word,count,eventTime,eventTime,eventTime);
      }
      resultData
    }
    //  第三步：处理数据
    val resultData=formatData.groupByKey(_.word)
      .mapGroupsWithState[ResultState,ResultData](GroupStateTimeout.ProcessingTimeTimeout())(mapGroupState)
    //  第四步：开始计算输出
    val query=resultData.writeStream.format("console").outputMode(OutputMode.Update()).start();
    //  第五步：等待计算完毕
    query.awaitTermination();

    spark.close();
  }

  /**
    * flatMapGroupsWithState操作
    * */
  @Test
  def flatMapGroup(): Unit ={

    val flatMapGroupState=(word:String,iter:Iterator[FormatData],state:GroupState[ResultState])=>{
      //由于Iterator集合的特点，此处将Iterator集合转化为list集合
      val list=iter.toList;
      val resultData=if(state.exists){
        //不是第一个出现
        if(state.hasTimedOut){
          state.remove();
          ResultData(word,0,null,null,null);
        }else{
          //时间未到，合并状态
          val oldState=state.get;
          val count=oldState.count+list.size
          val firstTime=oldState.firstTime;
          val beforeTime=oldState.beforeTime;
          val max=list.head.time;
          //更新状态
          val newState=ResultState(word,count,firstTime,max);
          state.update(newState);
          //设置超时时间
          state.setTimeoutDuration("30 seconds")
          ResultData(word,count,firstTime,beforeTime,max);
        }
      }else{
        //第一次出现,创建新ResultData对象
        //获取单词出现的次数
        val count=list.size;
        //获取事件时间 第一次min=beforeTime=max
        val eventTime=list.head.time;
        //更新状态
        val newState=ResultState(word,count,eventTime,eventTime);
        state.update(newState);
        //设置超时时间
        state.setTimeoutDuration("30 seconds")
        ResultData(word,count,eventTime,eventTime,eventTime);
      }
      List(resultData).toIterator
    }

    //  第三步：处理数据
    //想要使用mapGroupsWithState/flatMapGroupsWithState，必须先获取到KeyValueGroupedDataset对象
    val resultData=formatData.groupByKey(_.word)
      .flatMapGroupsWithState[ResultState,ResultData](OutputMode.Append(),GroupStateTimeout.ProcessingTimeTimeout())(flatMapGroupState)

    //  第四步：开始计算输出
    val query=resultData.writeStream.format("console").start();
    //  第五步：等待计算完毕
    query.awaitTermination();
    spark.close();

  }
}
