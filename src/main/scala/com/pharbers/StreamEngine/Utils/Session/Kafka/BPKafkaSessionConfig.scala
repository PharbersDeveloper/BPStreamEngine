package com.pharbers.StreamEngine.Utils.Session.Kafka

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

trait BPKafkaSessionConfig {
    val defaultKafkaURL = "http://123.56.179.133:9092"
    val defaultSchemaURL = "http://123.56.179.133:8081"
    val defaultKafkaTopic = "oss_source_1"

    final val KAFKA_URL_KEY = "url"
    final val KAFKA_URL_DOC = "kafka url"
    final val SCHEMA_URL_KEY = "schema"
    final val SCHEMA_URL_DOC = "Schema Registry url"
    final val TOPIC_KEY = "topic"
    final val TOPIC_DOC = "kafka topic config"

    final val configDef: ConfigDef = new ConfigDef()
            .define(KAFKA_URL_KEY, Type.STRING, defaultKafkaURL, Importance.HIGH, KAFKA_URL_DOC)
            .define(SCHEMA_URL_KEY, Type.STRING, defaultSchemaURL, Importance.HIGH, SCHEMA_URL_DOC)
            .define(TOPIC_KEY, Type.STRING, defaultKafkaTopic, Importance.HIGH, TOPIC_DOC)
}
