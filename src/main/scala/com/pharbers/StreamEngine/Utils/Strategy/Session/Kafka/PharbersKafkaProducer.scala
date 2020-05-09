package com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka

import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.Future

import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Module.baseModules.PharbersInjectModule
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.clients.producer._

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/06 18:30
  * @note 一些值得注意的地方
  */
object PharbersKafkaProducer {
    lazy val default_producer: PharbersKafkaProducer[String, Array[Byte]] = new PharbersKafkaProducer[String, Array[Byte]]()

    def apply: PharbersKafkaProducer[String, Array[Byte]] = default_producer
    //    def apply[K, V]: PharbersKafkaProducer[K, V] = new PharbersKafkaProducer[K, V]()
}

class PharbersKafkaProducer[K, V] extends PhLogable{

    lazy val config = new Properties()
    config.put(ProducerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost.getHostName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_config_obj.broker)
    config.put(ProducerConfig.ACKS_CONFIG, kafka_config_obj.acks)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafka_config_obj.keyDefaultSerializer)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafka_config_obj.valueDefaultSerializer)
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafka_config_obj.schemaRegistryUrl)
    config.put("security.protocol", kafka_config_obj.securityProtocol)
    config.put("ssl.endpoint.identification.algorithm", kafka_config_obj.sslAlgorithm)
    config.put("ssl.truststore.location", kafka_config_obj.sslTruststoreLocation)
    config.put("ssl.truststore.password", kafka_config_obj.sslTruststorePassword)
    config.put("ssl.keystore.location", kafka_config_obj.sslKeystoreLocation)
    config.put("ssl.keystore.password", kafka_config_obj.sslKeystorePassword)
    val producer = new KafkaProducer[K, V](config)


    def produce(topic: String, key: K, value: V): Future[RecordMetadata] = {
        val record: ProducerRecord[K, V] = new ProducerRecord[K, V](topic, key, value)
//        val fu = producer.send(record)
        val future: Future[RecordMetadata] = producer.send(record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                if (exception != null) logger.debug("Send failed for record {}", exception) else logger.info("SUCCEED!")
            }
        })
        future
    }

}

object kafka_config_obj extends PharbersInjectModule {
    override val id: String = "kafka-config"
    override val configPath: String = scala.util.Properties.envOrElse("PHA_CONF_HOME", "pharbers_config") + "/kafka_config.xml"
    override val md = "broker":: "group" :: "topics" :: "acks" ::
            "keyDefaultSerializer" :: "valueDefaultSerializer" ::
            "keyDefaultDeserializer" :: "valueDefaultDeserializer" ::
            "schemaRegistryUrl" :: "specificAvroReader" ::
            "securityProtocol" :: "sslAlgorithm" ::
            "sslTruststoreLocation" :: "sslTruststorePassword" ::
            "sslKeystoreLocation" :: "sslKeystorePassword" ::
            Nil

    lazy val broker: String = config.mc.find(p => p._1 == "broker").get._2.toString
    lazy val group: String = config.mc.find(p => p._1 == "group").get._2.toString
    lazy val topics: Array[String] = config.mc.find(p => p._1 == "topics").get._2.toString.split("##")
    lazy val acks: String = config.mc.find(p => p._1 == "acks").get._2.toString
    lazy val keyDefaultSerializer: String = config.mc.find(p => p._1 == "keyDefaultSerializer").get._2.toString
    lazy val valueDefaultSerializer: String = config.mc.find(p => p._1 == "valueDefaultSerializer").get._2.toString
    lazy val keyDefaultDeserializer: String = config.mc.find(p => p._1 == "keyDefaultDeserializer").get._2.toString
    lazy val valueDefaultDeserializer: String = config.mc.find(p => p._1 == "valueDefaultDeserializer").get._2.toString
    lazy val schemaRegistryUrl: String = config.mc.find(p => p._1 == "schemaRegistryUrl").get._2.toString
    lazy val specificAvroReader: String = config.mc.find(p => p._1 == "specificAvroReader").get._2.toString
    lazy val securityProtocol: String = config.mc.find(p => p._1 == "securityProtocol").get._2.toString
    lazy val sslAlgorithm: String = config.mc.find(p => p._1 == "sslAlgorithm").get._2.toString
    lazy val sslTruststoreLocation: String = config.mc.find(p => p._1 == "sslTruststoreLocation").get._2.toString
    lazy val sslTruststorePassword: String = config.mc.find(p => p._1 == "sslTruststorePassword").get._2.toString
    lazy val sslKeystoreLocation: String = config.mc.find(p => p._1 == "sslKeystoreLocation").get._2.toString
    lazy val sslKeystorePassword: String = config.mc.find(p => p._1 == "sslKeystorePassword").get._2.toString
}


