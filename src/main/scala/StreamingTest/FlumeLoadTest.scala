package StreamingTest

import java.net.InetSocketAddress

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumeLoadTest {
  def main(args: Array[String]): Unit = {
    println("create spark")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("flumeLoadTest")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

//    val flumeStream = FlumeUtils.createStream(ssc, "localhost", 8988)
//
//    flumeStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }
}
