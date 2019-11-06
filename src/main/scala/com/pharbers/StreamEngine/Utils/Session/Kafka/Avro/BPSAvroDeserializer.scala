package com.pharbers.StreamEngine.Utils.Session.Kafka.Avro

import org.apache.avro.Schema
import collection.JavaConverters._
import org.apache.spark.sql.types.DataType
import org.apache.avro.generic.GenericRecord
import com.databricks.spark.avro.SchemaConverters
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import com.pharbers.StreamEngine.Utils.Session.Kafka.BPKafkaSessionConfig
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

object BPSAvroDeserializer extends BPKafkaSessionConfig {
    def apply(kafkaUrl: String, schemaRegistryUrl: String): BPSAvroDeserializer =
        new BPSAvroDeserializer(kafkaUrl, schemaRegistryUrl)

    def apply(config: Map[String, String]): BPSAvroDeserializer = {
        val kafkaConfigs = new BPSConfig(configDef, config.asJava)
        new BPSAvroDeserializer(
            kafkaConfigs.getString(KAFKA_URL_KEY),
            kafkaConfigs.getString(SCHEMA_URL_KEY)
        )
    }
}

class BPSAvroDeserializer(kafkaUrl: String, schemaRegistryUrl: String) extends AbstractKafkaAvroDeserializer {
    override def deserialize(bytes: Array[Byte]): String = {
        val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
        genericRecord.toString
    }

    def getSchema(topic: String): DataType = {
        val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
        SchemaConverters.toSqlType(
            new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema)
        ).dataType
    }
}
