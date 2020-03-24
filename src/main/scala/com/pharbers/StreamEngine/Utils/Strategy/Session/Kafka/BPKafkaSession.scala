package com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka

import collection.JavaConverters._
import org.apache.spark.sql.types.DataType
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession._
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.Avro.BPSAvroDeserializer

object BPKafkaSession {
    def apply(compoentProperty: Component2.BPComponentConfig): BPKafkaSession = {
        val tmp = new BPKafkaSession(compoentProperty)
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }
}

/** 创建 KafkaSession 实例
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:37
 * @node 可用的配置参数
 * {{{
 *     url = kafka url
 *     schema = schema url
 *     topic = topic name
 * }}}
 * @example {{{val kafka = new BPKafkaSession(Map("topic" -> "test"))}}}
 */
@Component(name = "BPKafkaSession", `type` = "session")
class BPKafkaSession(override val componentProperty: Component2.BPComponentConfig) extends BPKafkaSessionConfig {
    private val kafkaConfigs =
        if (componentProperty != null) BPSConfig(configDef, componentProperty.config.asJava)
        else BPSConfig(configDef, Map.empty[String, String])

    lazy val kafkaUrl: String = kafkaConfigs.getString(KAFKA_URL_KEY)
    lazy val schemaRegistryUrl: String = kafkaConfigs.getString(SCHEMA_URL_KEY)
    lazy val topic: String = kafkaConfigs.getString(TOPIC_KEY)
    lazy val sparkSchema: DataType = BPSAvroDeserializer.getSchema(topic)
    override val sessionType: String = "kafka"
}
