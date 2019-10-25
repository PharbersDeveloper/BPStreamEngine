package com.pharbers.StreamEngine.Utils.Session.Kafka


import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Config.AppConfig
import com.pharbers.StreamEngine.Utils.Session.Kafka.Avro.BPSAvroDeserializer
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType

object BPKafkaSession {
//    def apply(spark: SparkSession): BPKafkaSession = {
//        val tmp = new BPKafkaSession(Map.empty)
//        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
//        tmp
//    }

    def apply(spark: SparkSession, config: Map[String, String]): BPKafkaSession = {
        val tmp = new BPKafkaSession(config)
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }
}
@Component(name = "BPKafkaSession", `type` = "session")
class BPKafkaSession(config: Map[String, String]) extends KafkaConfig{
    final private val KAFKA_URL = "url"
    final private val KAFKA_URL_DOC = "kafka url"
    final private val SCHEMA_URL = "schema"
    final private val SCHEMA_URL_DOC = "Schema Registry url"
    configDef.define(KAFKA_URL, Type.STRING, "http://123.56.179.133:9092", Importance.HIGH, KAFKA_URL_DOC)
            .define(SCHEMA_URL, Type.STRING, "http://123.56.179.133:8081", Importance.HIGH, SCHEMA_URL_DOC)
    val kafkaConfig: AppConfig = new AppConfig(configDef,  config.asJava)

    lazy val topic: String = kafkaConfig.getString(TOPIC)
    lazy val kafkaUrl: String = kafkaConfig.getString(KAFKA_URL)
    lazy val schemaRegistryUrl: String = kafkaConfig.getString(SCHEMA_URL)
    lazy val sparkSchema: DataType = BPSAvroDeserializer.getSchema(topic)
}

trait KafkaConfig{
    final val TOPIC = "topic"
    final val TOPIC_DOC = "kafka topic config"
    val configDef: ConfigDef = new ConfigDef().define(TOPIC, Type.STRING, "oss_source", Importance.HIGH, TOPIC_DOC)
}
