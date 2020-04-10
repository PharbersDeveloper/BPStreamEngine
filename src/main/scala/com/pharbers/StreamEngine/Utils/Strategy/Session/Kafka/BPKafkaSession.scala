package com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka

import java.util.concurrent.TimeUnit

import collection.JavaConverters._
import org.apache.spark.sql.types.DataType
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession._
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.Avro.BPSAvroDeserializer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.EventMsg

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
    lazy val dataTopic: String = kafkaConfigs.getString(DATA_TOPIC_KEY)
    lazy val msgTopic: String = kafkaConfigs.getString(MSG_TOPIC_KEY)
    lazy val sparkSchema: DataType = BPSAvroDeserializer.getSchema(dataTopic)
    val pkp = new PharbersKafkaProducer[String, EventMsg]
    override val sessionType: String = "kafka"

    def getDataTopic: String = this.dataTopic
    def getMsgTopic: String = this.msgTopic
    def getSchema: org.apache.spark.sql.types.DataType = this.sparkSchema

    def callKafka(msg: BPSEvents): Unit ={
        val res = pkp.produce(msgTopic, "", new EventMsg(msg.jobId, msg.traceId, msg.`type`, msg.data))
        logger.info(res.get(10, TimeUnit.SECONDS))
    }
}
