
import java.util.UUID

import org.apache.spark.SparkConf
import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.AvroDeserializer.AvroDeserializer

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

    val spark = SparkSession.builder()
        .config(conf).getOrCreate()
    import spark.implicits._

    spark.sparkContext.addFile("./kafka.broker1.keystore.jks")
    spark.sparkContext.addFile("./kafka.broker1.truststore.jks")
    spark.sparkContext.addJar("./target/BP-Stream-Engine-1.0-SNAPSHOT.jar")
    spark.sparkContext.addJar("./jars/kafka-schema-registry-client-5.2.1.jar")
    spark.sparkContext.addJar("./jars/kafka-avro-serializer-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-config-5.2.1.jar")
    spark.sparkContext.addJar("./jars/common-utils-5.2.1.jar")

    import spark.implicits._

    lazy val topic = "oss_source_1"
    lazy val kafkaUrl = "http://123.56.179.133:9092"
    lazy val schemaRegistryUrl = "http://123.56.179.133:8081"

    lazy val sparkSchema = AvroDeserializer.getSchema(topic)

    spark.udf.register("deserialize", (bytes: Array[Byte]) => AvroDeserializer(bytes))

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

    val selectDf = logsDf
            .selectExpr(
                """deserialize(value) AS value""",
                "timestamp"
            ).toDF()
        .withWatermark("timestamp", "24 hours")
        .select(
            from_json($"value", sparkSchema.dataType).as("data")
        ).select("data.*")

    val jobId = UUID.randomUUID()
    val path = "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files"

    val query = selectDf.writeStream
         .outputMode("append")
//        .outputMode("complete")
//         .format("console")
        .format("csv")
        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
        .option("path", "/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
