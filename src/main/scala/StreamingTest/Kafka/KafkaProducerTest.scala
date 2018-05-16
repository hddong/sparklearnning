package StreamingTest.Kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer

/**
  * function:
  *    kafka 发送消息测试
  */
object KafkaProducerTest {
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

    for (i <- 0 to 10) {
      producer.send(new ProducerRecord(topic, "key-" + i, "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846"))
      Thread.sleep(3000)
    }

    producer.close()

    /*
     * kafka 旧的写法,多种函数已被注释不可用
     *
     * val props = new Properties()
     * props.put("metadata.broker.list", brokers)
     * props.put("serializer.class", classOf[StringEncoder].getName)
     * props.put("request.required.acks", "1")
     * props.put("producer.type", "async")
//    val config = new ProducerConfig(props)
//    val producer = new Producer[String, String](config)
//
//    val message = new KeyedMessage[String, String](topic, "1", "some thing that is wrong")
//    producer.send(message)
    */
  }
}
