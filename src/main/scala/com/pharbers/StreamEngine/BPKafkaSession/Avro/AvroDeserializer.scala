package com.pharbers.StreamEngine.BPKafkaSession.Avro

import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object AvroDeserializer {
    lazy val kafkaUrl = "http://123.56.179.133:9092"
    lazy val schemaRegistryUrl = "http://123.56.179.133:8081"

    lazy val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    lazy val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

    def apply(bytes: Array[Byte]): String = {
        this.kafkaAvroDeserializer.deserialize(bytes)
    }

    def getSchema(topic: String) =
        SchemaConverters.toSqlType(
            new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema)
        ).dataType
}

class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
        this()
        this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
        val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
        genericRecord.toString
    }
}