package com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka

import com.pharbers.StreamEngine.Utils.Strategy.Session.SessionStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

trait BPKafkaSessionConfig extends SessionStrategy {
    val defaultKafkaURL = "http://123.56.179.133:9092"
    val defaultSchemaURL = "http://123.56.179.133:8081"
    val defaultDataKafkaTopic = "oss_source"
    val defaultMsgKafkaTopic = "oss_msg"

    final val KAFKA_URL_KEY = "url"
    final val KAFKA_URL_DOC = "kafka url"
    final val SCHEMA_URL_KEY = "schema"
    final val SCHEMA_URL_DOC = "Schema Registry url"
    final val DATA_TOPIC_KEY = "data.topic"
    final val DATA_TOPIC_DOC = "kafka data topic config"
    final val MSG_TOPIC_KEY = "msg.topic"
    final val MSG_TOPIC_DOC = "kafka msg topics config"

    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(KAFKA_URL_KEY, Type.STRING, defaultKafkaURL, Importance.HIGH, KAFKA_URL_DOC)
            .define(SCHEMA_URL_KEY, Type.STRING, defaultSchemaURL, Importance.HIGH, SCHEMA_URL_DOC)
            .define(DATA_TOPIC_KEY, Type.STRING, defaultDataKafkaTopic, Importance.HIGH, DATA_TOPIC_DOC)
            .define(MSG_TOPIC_KEY, Type.STRING, defaultMsgKafkaTopic, Importance.HIGH, MSG_TOPIC_DOC)
}
