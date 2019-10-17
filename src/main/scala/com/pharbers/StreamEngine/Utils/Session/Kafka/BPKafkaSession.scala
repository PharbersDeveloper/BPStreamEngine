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
    def apply(spark: SparkSession): BPKafkaSession = {
        val tmp = new BPKafkaSession()
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }

    def apply(spark: SparkSession, config: Map[String, String]): BPKafkaSession = {
        val tmp = new BPKafkaSession(config.asJava)
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }
}
@Component(name = "BPKafkaSession", `type` = "session")
class BPKafkaSession() extends KafkaConfig{
    final private val KAFKA_URL = "url"
    final private val KAFKA_URL_DOC = "kafka url"
    override val configDef: ConfigDef = super.configDef.define(KAFKA_URL, Type.STRING, Importance.HIGH, KAFKA_URL_DOC)
    //todo: 因为时demo，先不改变之前的逻辑，之后替换
    var kafkaConfig: Option[AppConfig] = None
    def this(config: java.util.Map[_, _]){
        this
        kafkaConfig = Some(new AppConfig(configDef, config))
    }
    lazy val topic: String = kafkaConfig match {
        case Some(c) => c.getString(TOPIC)
        case _ => "oss_source"
    }
    lazy val kafkaUrl = "http://123.56.179.133:9092"
    lazy val schemaRegistryUrl = "http://123.56.179.133:8081"
    lazy val sparkSchema: DataType = BPSAvroDeserializer.getSchema(topic)
}

trait KafkaConfig{
    final val TOPIC = "topic"
    final val TOPIC_DOC = "kafka topic config"
    val configDef: ConfigDef = new ConfigDef().define(TOPIC, Type.STRING, Importance.HIGH, TOPIC_DOC)
}
