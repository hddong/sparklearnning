package StreamingTest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaGetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kafkaGetTest")
      .master("local[2]")
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    /**
      * kafka 10 新写法
      * 参考: http://spark.apache.org/docs/latest/streaming-kafka-integration.html
      *    http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
      */
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.16.60.186:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_test_kafka",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("test")

    val kafkaStream = KafkaUtils
      .createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val rest = kafkaStream.map(_.value)
    rest.print(10)
    /**
      * kafka 0.8版本写法

    //    val topic = Set("test")
//    val brokers = "172.16.60.186:9092"
//    val kafkaParams = Map[String,String](
//      "metadata.broker.list" -> brokers,
//      "serializer.class" -> "kafka.serializer.StringEncoder"
//    )
//
//    val kafkaStream = KafkaUtils
//      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
//
//    val rest = kafkaStream.map(line => line._2)
//    rest.print(10)
      */
    ssc.start()
    ssc.awaitTermination()
  }
}
