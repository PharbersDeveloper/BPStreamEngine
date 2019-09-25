
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession

object main extends App {
    val yarnJars: String = "hdfs://spark.master:9000/jars/sparkJars"

    private val conf = new SparkConf()
        .set("spark.yarn.jars", yarnJars)
        .set("spark.yarn.archive", yarnJars)
        .setAppName("bp-stream-engine")
        .setMaster("yarn")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.executor.memory", "1g")
        .set("spark.executor.cores", "1")
        .set("spark.executor.instances", "1")

    val schema: StructType = StructType(Seq(
        StructField("Time", StringType),
        StructField("Hostname", StringType),
        StructField("ProjectName",StringType),
        StructField("File", StringType),
        StructField("Func", StringType),
        StructField("JobId", StringType),
        StructField("TraceId", StringType),
        StructField("UserId", StringType),
        StructField("Message", StringType),
        StructField("Level", StringType)
    ))

    val spark = SparkSession.builder()
        .config(conf).getOrCreate()

    import spark.implicits._

    private val topic = "oss_source_1"

    private val schemaRegistryUrl = "http://123.56.179.133:8081"

    private val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)

    private val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
    private val sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

    spark.sparkContext.addFile("kafka.broker1.keystore.jks")
    spark.sparkContext.addFile("kafka.broker1.truststore.jks")
//    spark.sparkContext.addJar("target/jar-all.jar")

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
        DeserializerWrapper.deserializer.deserialize(bytes)
    )

    val logsDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "123.56.179.133:9092")
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
        .option("kafka.ssl.keystore.password", "pharbers")
        .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
        .option("kafka.ssl.truststore.password", "pharbers")
        .option("kafka.ssl.endpoint.identification.algorithm", " ")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    import org.apache.spark.sql.functions._
    val selectDf = logsDf
            .selectExpr("""deserialize(value) AS value""")
//            .selectExpr(
//        "CAST(key AS STRING)",
//        "CAST(value AS STRING)",
//        "timestamp").as[(String, String, String)].toDF()
//        .withWatermark("timestamp", "24 hours")
        .select(
            from_json($"value", sparkSchema.dataType).as("data")
        ).select("data.*")

    val jobId = UUID.randomUUID()
    val query = selectDf.writeStream
         .outputMode("append")
//         .format("console")
        .format("csv")
        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
        .option("path", "/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()

}

object DeserializerWrapper {
    private val kafkaUrl = "http://123.56.179.133:9092"
    private val schemaRegistryUrl = "http://123.56.179.133:8081"

    private val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
    private val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)
    val deserializer = kafkaAvroDeserializer
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
