package com.pharbers.StreamEngine.Utils.Session.Kafka

import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Session.Kafka.Avro.BPSAvroDeserializer

object BPKafkaSession {
    def apply(spark: SparkSession, config: Map[String, String]): BPKafkaSession = {
        val tmp = new BPKafkaSession(config)
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }
}

/** 创建 KafkaSession 实例
 *
 * @author clock
 * @version 0.1
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
class BPKafkaSession(config: Map[String, String]) extends BPKafkaSessionConfig {
    val kafkaConfigs: BPSConfig = new BPSConfig(configDef, config.asJava)

    lazy val kafkaUrl: String = kafkaConfigs.getString(KAFKA_URL_KEY)
    lazy val schemaRegistryUrl: String = kafkaConfigs.getString(SCHEMA_URL_KEY)
    lazy val topic: String = kafkaConfigs.getString(TOPIC_KEY)
    lazy val sparkSchema: DataType = BPSAvroDeserializer.getSchema(topic)
}
