//package StreamingTest
//
//import java.util.regex.{Matcher, Pattern}
//
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//
//object SparkSQLStreamingTest {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder()
//      .appName("LogAnalyzerTest")
//      .master("local[2]")
//      .getOrCreate()
//    Logger.getRootLogger.setLevel(Level.WARN)
//
//    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
//
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "172.16.60.186:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "spark_test_kafka",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
//    val topics = Array("test")
//
//    val kafkaStream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
//
//    val newStream = kafkaStream.map(line => transformLog(line.value))
//
////    val wStream = newStream.window(Seconds(10), Seconds(4))
//    /*
//    所谓的DStream，或者说Discretized Stream指的是将连续的流数据分成小块批数据的抽象。
//    这就是我们上面说的mini-batch过程。每一个mini-batch体现为一个所谓的RDD（Resilient Distributed Dataset）。
//    而RDD被交给Spark executor进行进一步的处理。对每一个mini-batch间隔对应的DStream来说，有且仅有一个RDD被产生。
//    一个RDD是一份分布式的数据集。我们可以把RDD当成指向集群中真正数据块的指针。
//    DStream.foreachRDD()方法实际上是Spark流处理的一个处理及输出RDD的方法。
//    这个方法使我们能够访问底层的DStream对应的RDD进而根据我们需要的逻辑对其进行处理。
//    例如，我们可以通过foreachRDD()方法来访问每一条mini-batch中的数据，然后将它们存入数据库。
//     */
//    newStream.foreachRDD{ rdd =>
//      val sparkSession = SparkSession.builder().getOrCreate()
//      sparkSession.createDataFrame(rdd).show()
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//
//  def transformLog(logLine: String)/* tuple9*/ = {
//    // 抽取数据的相关模式 正则匹配
//    val LOG_ENTRY_PATTERN =
//      """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
//    val pattern = Pattern.compile(LOG_ENTRY_PATTERN)
//    val matcher = pattern.matcher(logLine)
//    if (!matcher.find()) {
//      println("no found, cannot convert!")
//    }
//    createDataString(matcher)
//  }
//  def createDataString(m: Matcher) = {
//    Tuple9.apply(m.group(1), m.group(2), m.group(3), m.group(4),
//      m.group(5), m.group(6), m.group(7), m.group(8), m.group(9)
//    )
//  }
//}
