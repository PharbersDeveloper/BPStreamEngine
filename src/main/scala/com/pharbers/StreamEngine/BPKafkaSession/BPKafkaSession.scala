package com.pharbers.StreamEngine.BPKafkaSession

import com.pharbers.StreamEngine.BPKafkaSession.Avro.AvroDeserializer
import org.apache.spark.sql.SparkSession

object BPKafkaSession {
    def apply(spark: SparkSession): BPKafkaSession = {
        val tmp = new BPKafkaSession()
        spark.udf.register("deserialize", (bytes: Array[Byte]) => AvroDeserializer(bytes))
        tmp
    }
}

class BPKafkaSession() {
    lazy val topic = "oss_source_1"
    lazy val kafkaUrl = "http://123.56.179.133:9092"
    lazy val schemaRegistryUrl = "http://123.56.179.133:8081"
    lazy val sparkSchema = AvroDeserializer.getSchema(topic)
}
