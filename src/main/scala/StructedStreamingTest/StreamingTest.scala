package StructedStreamingTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("structuredStreamingTest")
      .getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)
    val lines = spark.readStream
      .format("socket")
      .option("host", "192.168.56.101")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordsCounts = words.groupBy("value").count()
//    wordsCounts.for
//    val query = wordsCounts.writeStream
//      .outputMode("complete")
//      .format("console")
//      .trigger(Trigger.ProcessingTime("1 second"))
//      .start()
    val query = wordsCounts.writeStream
      .outputMode("complete")
      .format("json")
      .trigger(Trigger.ProcessingTime("1 second"))
      .option("checkpointLocation", "src/output/checkpoint")
      .start("src/output/test")
    query.awaitTermination()
  }
}
