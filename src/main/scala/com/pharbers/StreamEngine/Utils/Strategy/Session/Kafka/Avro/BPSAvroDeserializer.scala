package com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.Avro

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.avro.SchemaConverters

object BPSAvroDeserializer {

    //todo: 硬编码是不行的
    lazy val kafkaUrl = "http://broker-svc.message:9092"
    lazy val schemaRegistryUrl = "http://schema.message:8081"

    lazy val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    lazy val kafkaAvroDeserializer = new BPSAvroDeserializer(schemaRegistryClient)

    def apply(bytes: Array[Byte]): String = {
        this.kafkaAvroDeserializer.deserialize(bytes)
    }

    def getSchema(topic: String) =
        SchemaConverters.toSqlType(
            new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema)
        ).dataType
}

class BPSAvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
        this()
        this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
        val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
        genericRecord.toString
    }
}
