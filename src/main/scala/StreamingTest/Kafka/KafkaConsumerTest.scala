package StreamingTest.Kafka

import java.util.{Collections, Properties, UUID}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._  /* 隐式转换 将java collection转为scala */

/**
  * function:
  *    kafka consumer 测试
  */
object KafkaConsumerTest {

  def ZK_CONN = "192.168.56.171:2181"
  def BOOTSTRAP = "192.168.56.171:9092"
  def GROUP_ID = "test_scala_consumer_group"
  def TOPIC = "test"

  def main(args: Array[String]): Unit = {
    println("开始读取kafka数据")

    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    consumer.subscribe(Collections.singleton(TOPIC))

    consumer.seekToBeginning(consumer.assignment())
    val records = consumer.poll(10000)
//    println(records.count())
    for (record <- records.asScala) {
      println(record)
    }

    consumer.close()
  }

  def kafkaConfig(): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG /**/, BOOTSTRAP)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG /**/, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID)
//    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString) /* 随机groupId从头获取数据 */
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true.toString)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
      "earliest" /* latest表示接受接收最大的offset(即最新消息),earliest表示最小offset,即从topic的开始位置消费所有消息. */)
    props
  }
}
