package com.pharbers.StreamEngine.Utils.KafkaUtils

import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import collection.JavaConverters._
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.kafka_config_obj
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/07 10:34
  * @note 一些值得注意的地方
  */
class PharbersKafkaConsumer[K, V](val topics: List[String], val msgFrequencyMs: Long = Long.MaxValue, val permitsCount: Int = Int.MaxValue,
                                  val process: ConsumerRecord[K, V] => _)
        extends Runnable with PhLogable{

    val config = new Properties()
    config.put("client.id", InetAddress.getLocalHost.getHostName)
    config.put("group.id", kafka_config_obj.group)
    config.put("bootstrap.servers", kafka_config_obj.broker)
    config.put("key.deserializer", kafka_config_obj.keyDefaultDeserializer)
    config.put("value.deserializer", kafka_config_obj.valueDefaultDeserializer)
    config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, kafka_config_obj.specificAvroReader)
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, kafka_config_obj.schemaRegistryUrl);
//    config.put("security.protocol", kafka_config_obj.securityProtocol)
//    config.put("ssl.endpoint.identification.algorithm", kafka_config_obj.sslAlgorithm)
//    config.put("ssl.truststore.location", kafka_config_obj.sslTruststoreLocation)
//    config.put("ssl.truststore.password", kafka_config_obj.sslTruststorePassword)
//    config.put("ssl.keystore.location", kafka_config_obj.sslKeystoreLocation)
//    config.put("ssl.keystore.password", kafka_config_obj.sslKeystorePassword)

    final private val CONSUMER = new KafkaConsumer[K, V](config)
    final private val SHUTDOWN = new AtomicBoolean(false)
    final private val PERMITS = new Semaphore(permitsCount)

    override def run(): Unit = {
        try {
            //动态分配partition
            if (topics.nonEmpty) CONSUMER.subscribe(topics.asJava) else CONSUMER.subscribe(kafka_config_obj.topics.toList.asJava)
            //自己指定topic和partition，如果group内其他也分配这个partition，会导致offset错乱
            //if (topics.nonEmpty) CONSUMER.assign(List(new TopicPartition("DCS1", 2)).asJava) else CONSUMER.subscribe(kafka_config_obj.topics.toList.asJava)
            logger.info("Origin PERMITS_COUNT=" + PERMITS.availablePermits())

            while ( {
                !SHUTDOWN.get
            }) {
                val records = CONSUMER.poll(msgFrequencyMs)
                //                Log.info("The length of records=" + records.count())
                if (records.count() > PERMITS.availablePermits()) {
                    logger.error(s"There are not enough permits[count=${PERMITS.availablePermits()}] to consume records[count=${records.count()}]")
                    SHUTDOWN.set(true)
                }
                if (!records.isEmpty) PERMITS.acquire(records.count())
                records.asScala.foreach(process)
                //                Log.info("The rest of PERMITS_COUNT=" + PERMITS.availablePermits())
                if (PERMITS.availablePermits() <= 0) SHUTDOWN.set(true)
            }
        } finally {
            shutdown()
        }
    }

    def getConsumer: KafkaConsumer[K, V] ={
        CONSUMER
    }

    @throws[InterruptedException]
    private def shutdown(): Unit = {
        if (PERMITS.availablePermits() < 0) logger.warn("Excessive consumption! The rest of CONSUME_TIMES=" + PERMITS.availablePermits())
        CONSUMER.close()
    }

    def close(): Unit = {
        SHUTDOWN.set(true)
    }
}
