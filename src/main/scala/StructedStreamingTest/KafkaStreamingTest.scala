package StructedStreamingTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("kafkatest")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import spark.implicits._
    val read = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.56.171:9092")
      .option("subscribe", "test")
      .load()
    val data = read.selectExpr("CAST(key as STRING)",
      "CAST(value as string)",
      "cast(offset as long)",
      "cast(timestamp as long)")
      .withColumn("now", lit(System.currentTimeMillis()))
      .as[(String, String, Long, Long, Long)]
    val query = data.writeStream.format("console").start()
    query.awaitTermination()
  }
}
