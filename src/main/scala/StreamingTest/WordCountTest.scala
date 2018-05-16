package StreamingTest

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * function:
  *    spark streaming 词频统计
  */
object WordCountTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("streamingTest")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    // 从sparkConf 获取spark上下文 Seconds 表示批处理间隔
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    //
    // 定义Stream类型,这里使用TCP套接字作为文本流
    // 程序监听本地(localhost)端口,收到数据流后存入内存
    // 内存不足则存入硬盘
    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER_2)

    // 对获取的数据进行分割,得到DStream
    val words = lines.flatMap(_.split(" "))

    // 对得到的每个单词记为1,然后进行累加
    val wordCount = words.map((_, 1)).reduceByKey(_ + _)
    wordCount.print(20)

    ssc.start()
    ssc.awaitTermination()
  }
}
