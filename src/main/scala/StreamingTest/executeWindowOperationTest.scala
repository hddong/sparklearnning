package StreamingTest

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
  * function:
  *    spark streaming 窗口函数操作
  *    数据来源 kafka
  */
object executeWindowOperationTest {
  def main(args: Array[String]): Unit = {

  }
  /**
    * ps:
    *    在使用DStream window函数时,由于可能多个窗口访问同一数据流,而kafka又是线程不安全的,会报错
    *    java.util.ConcurrentModificationException:KafkaConsumer is not safe for multi-threaded access
    *
    *    这时有两种解决方案
    *    切断lineage
    *    1 对使用window操作的DStream在调用window之前先调用checkpoint方法，可以截断lineage，从而避免这个问题(网上找的方法，未测试)。
    *    ssc.checkpoint("D:/WorkSpace/hadoop/sparklearning/spark-warehouse")
  *    newStream.checkpoint(Seconds(2))  /* Seconds的值应该为窗口和滑动间隔的最小公约数 */
  *    2 在接收 kafka 数据后使用 repartition 切断 lineage;
  *    kafkaStream.repartition(4);
  */
  //
  // 窗口长度（window length），即窗口的持续时间  即当前窗口向前推几次(包含当次)
  // 滑动间隔（sliding interval），窗口操作执行的时间间隔
  // time1 --- time2 --- time3 --- time4 --- time5 ---
  // |                       |
  // |                       |
  //  ---------window1-------
  //                      |                       |
  //                      |                       |
  //                       ---------window2-------
  // 该图显示了 窗口长度为 3 滑动间隔为 2
  def execWindow(dStream: DStream[(String, String)], ssc: StreamingContext): Unit = {
//    ssc.checkpoint("D:/WorkSpace/hadoop/sparklearning/spark-warehouse")
//    dStream.checkpoint(Seconds(2))
    dStream.repartition(4)
    println("use windowing operation")
    val wStream = dStream.window(Seconds(8), Seconds(4))
    val respStream = wStream.filter(x => x._1.contains("respCode")).map(x => (x._2, 1))
    respStream.reduceByKey(_+_).print(100)

//    println("use reduceByKeyAndWindow")
//    respStream.reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(40), Seconds(20)).print(100)
//
//    println("use groupByKeyAndWindow")
//    respStream.groupByKeyAndWindow(Seconds(40), Seconds(20)).print(100)
  }
}
