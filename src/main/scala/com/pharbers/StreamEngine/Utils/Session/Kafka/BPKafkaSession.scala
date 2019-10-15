package com.pharbers.StreamEngine.Utils.Session.Kafka

import com.pharbers.StreamEngine.Utils.Session.Kafka.Avro.BPSAvroDeserializer
import org.apache.spark.sql.SparkSession

object BPKafkaSession {
    def apply(spark: SparkSession): BPKafkaSession = {
        val tmp = new BPKafkaSession()
        spark.udf.register("deserialize", (bytes: Array[Byte]) => BPSAvroDeserializer(bytes))
        tmp
    }
}

class BPKafkaSession() {
//    lazy val topic = "oss_source_1"
    lazy val topic = "oss_topic_1"
    lazy val kafkaUrl = "http://123.56.179.133:9092"
    lazy val schemaRegistryUrl = "http://123.56.179.133:8081"
    lazy val sparkSchema = BPSAvroDeserializer.getSchema(topic)
}
