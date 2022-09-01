package src.main.scala.org.example.stream.state

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming._

/**
  *计算流式数据中,不超过某一固定时间内的词汇出现的个数,以及间隔时间(最后一次的时间-第一次出现的时间)
  * 超过固定时间的词汇删除
  * 需要借助于:org.apache.spark.sql.streaming.GroupState[T]这个特质,可以去查看此特质。
  * +-----+----------+---------+-------+
    |   id|durationMs|numEvents|expired|
    +-----+----------+---------+-------+
    |spark|        92|        8|  false|
    |   my|         0|        1|  false|
    +-----+----------+---------+-------+
  * */
object StructuredSessionization {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql")
      .enableHiveSupport().getOrCreate()
    import spark.implicits._

    val lines=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .option("includeTimestamp", true)
      .load()

    //从网络中接受文本数据，将数据转化封装为Event对象
    val events = lines.as[(String, Timestamp)]
           .flatMap { case (line, timestamp) =>
               line.split(" ").map(word => Event(sessionId = word, timestamp))
           }

    //讨论事件。跟踪事件的数量、会话的开始和结束时间戳以及报告会话更新。
    val sessionUpdates: Dataset[SessionUpdate] = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

      case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>
        //如果超时，则删除会话并发送最终更新。
        if (state.hasTimedOut) {

          val finalUpdate = SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
          //之前的状态对象
//        val sessionInfo=state.get;
//          SessionUpdate("",0L,0,true)
        } else {
          //在会话中更新开始和结束时间戳
          val timestamps = events.map(_.timestamp.getTime).toSeq
//          timestamps.foreach(x=>println(x))

          val updatedSession = if (state.exists) {
            //之前已经出现过这个sessionId
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            //第一次 设置状态对象
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          //更新状态
          state.update(updatedSession)
          //设置超时，如果10秒没有接受到新的数据，会话将过期 10 seconds  1 hour 2 days
          state.setTimeoutDuration("10 seconds")
          //设置结果
          SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    //开始运行将会话更新打印到控制台的查询操作
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
/** 表示输入事件的用户定义数据类型*/
case class Event(sessionId: String, timestamp: Timestamp)

/**
  * 用户定义的数据类型，用于将会话信息作为状态存储在mapGroupsWithState中。
  *
  * @param numEvents        total number of events received in the session
  * @param startTimestampMs timestamp of first event received in the session when it started
  * @param endTimestampMs   timestamp of last event received in the session before it expired
  */
case class SessionInfo(
                        numEvents: Int,
                        startTimestampMs: Long,
                        endTimestampMs: Long) {

  /** 会话的持续时间，在第一个和最后一个事件之间*/
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
  * 用户定义的数据类型，表示由mapGroupsWithState返回的更新信息。
  *
  * @param id          Id of the session
  * @param durationMs  Duration the session was active, that is, from first event to its expiry
  * @param numEvents   Number of events received by the session while it was active
  * @param expired     Is the session active or expired
  */
case class SessionUpdate(
                          id: String,
                          durationMs: Long,
                          numEvents: Int,
                          expired: Boolean)


