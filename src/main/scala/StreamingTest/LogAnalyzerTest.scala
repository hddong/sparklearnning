package StreamingTest

import java.util.regex.{Matcher, Pattern}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * function:
  *    日志分析测试,apache log
  *    下载地址: http://www.monitorware.com/en/logsamples/download/apache-samples.rar
  * ps:
  *    在使用DStream window函数时,由于可能多个窗口访问同一数据流,而kafka又是线程不安全的,会报错
  *    java.util.ConcurrentModificationException:KafkaConsumer is not safe for multi-threaded access
  *
  *    解决方案
  *    两种方案切断lineage
  *
  *    1 对使用window操作的DStream在调用window之前先调用checkpoint方法，可以截断lineage，从而避免这个问题(网上找的方法，未测试)。
  *    ssc.checkpoint("D:/WorkSpace/hadoop/sparklearning/spark-warehouse")
  *    newStream.checkpoint(Seconds(2))  /* Seconds的值应该为窗口和滑动间隔的最小公约数 */
  *
  *    2 在接收 kafka 数据后使用 repartition 切断 lineage (测试无效,不知什么原因,有空研究下)
  *    kafkaStream.repartition(4);
  */
object LogAnalyzerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LogAnalyzerTest")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.60.186:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_test_kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")

    val kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

//    val newStream = kafkaStream.flatMap{line =>
//      transformLogData(line.value)
//    }.filter(_._1.contains("method")).map(x => (x._2, 1)).reduceByKey(_+_)
//    newStream.print(20)
    val newStream = kafkaStream.flatMap(line => transformLogData(line.value))
//    ssc.checkpoint("D:/WorkSpace/hadoop/sparklearning/spark-warehouse")
//    newStream.checkpoint(Seconds(4))
//    newStream.window(Seconds(10), Seconds(4))
//      .filter(x => x._1.contains("respCode"))
//      .map(x => (x._2, 1))
//      .reduceByKey(_+_)
//      .print(100)
    executeWindowOperationTest.execWindow(newStream, ssc)

    ssc.start()
    ssc.awaitTermination()

//    val test = "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"
//    println(test)
//    println(transformLogData(test))
  }


  /**
    * 将日志转换为 key/value 对
    */
  def transformLogData(logLine: String): Map[String, String] = {
    // 抽取数据的相关模式 正则匹配
    val LOG_ENTRY_PATTERN =
      """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
    val pattern = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = pattern.matcher(logLine)
    if (!matcher.find()) {
      println("no found, cannot convert!")
    }
    createDataMap(matcher)
  }

  def createDataMap(m:Matcher): Map[String, String] = {
    Map[String, String](
      "ip" -> m.group(1),
      "client" -> m.group(2),
      "user" ->   m.group(3),
      "date" -> m.group(4),
      "method" -> m.group(5),
      "request" -> m.group(6),
      "protocol" -> m.group(7),
      "respCode" -> m.group(8),
      "size" -> m.group(9)
    )
  }
}
