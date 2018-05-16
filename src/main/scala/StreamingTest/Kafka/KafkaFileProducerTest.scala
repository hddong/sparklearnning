package StreamingTest.Kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaFileProducerTest {
  def main(args: Array[String]): Unit = {
    val brokers = "172.16.60.186:9092"
    val topic = "test"

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG /* 同"key.serializer"必须配置*/,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG /* 同"value.serializer"必须配置*/,
      classOf[StringSerializer].getName)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG /* 同"bootstrap.servers" 必须配置*/, brokers)
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")

    val producer = new KafkaProducer[String, String](props)

    // 读取文件并用kafka发送消息
    val file = Source.fromFile("./src/main/resources/StreamingData/access_log")
    for (line <- file.getLines) {
      producer.send(new ProducerRecord(topic, "key", line))
      Thread.sleep(500)
    }
//
//    for (i <- 0 to 10) {
//      producer.send(new ProducerRecord(topic, "key-" + i, "message-" + i))
//      Thread.sleep(3000)
//    }
//
    producer.close()
  }
}
