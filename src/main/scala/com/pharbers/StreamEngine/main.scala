
import java.util.UUID

import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import com.pharbers.StreamEngine.schemaReg.SchemaReg
import org.apache.avro.Schema

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.udf

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
        .set("spark.sql.avro.compression.codec", "deflate")
        .set("spark.sql.avro.deflate.level", "5")

    val spark = SparkSession.builder()
        .config(conf).getOrCreate()

    spark.sqlContext.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "true")

    spark.sparkContext.addFile("./kafka.broker1.keystore.jks")
    spark.sparkContext.addFile("./kafka.broker1.truststore.jks")
    spark.sparkContext.addJar("./jars/spark-avro_2.11-2.4.4.jar")
    spark.sparkContext.addJar("./target/BP-Stream-Engine-1.0-SNAPSHOT.jar")

    import spark.implicits._

    val logsDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "123.56.179.133:9092")
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
        .option("kafka.ssl.keystore.password", "pharbers")
        .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
        .option("kafka.ssl.truststore.password", "pharbers")
        .option("kafka.ssl.endpoint.identification.algorithm", " ")
//        .option("subscribe", "test001")
        .option("subscribe", "oss_source_1")
        .option("startingOffsets", """{"oss_source_1":{"0":-2}}""")
        .load()

    val selectDf = logsDf
        .selectExpr(
        "CAST(key AS STRING)",
//        "CAST(value AS STRING)",
        "value",
        "timestamp").toDF()
        .withWatermark("timestamp", "24 hours").select("value")
//        .as[Array[Byte]]
//        .as[String]

    val decodeAvro = udf { x: Array[Byte] =>
        println("=======>" + x)
        val schema = Schema.parse(SchemaReg.tmp)
        println("=======>" + schema)
        val reader1 = new GenericDatumReader[GenericRecord](schema)

        val decoder1 = DecoderFactory.get.binaryDecoder(x, null)
        val result = reader1.read(null, decoder1)
        println("=======>" + result)
        println("=======>" + result.toString)
        result.toString
    }

    val decodeDf = selectDf.withColumn("value", decodeAvro('value))

    val result = decodeDf.toDF().select(
        from_json($"value", SchemaReg.baseSchema).as("data")
    ).select("data.*")

    val sch = result.printSchema()

    val jobId = UUID.randomUUID()
    val path = "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files"

    val query = decodeDf.writeStream
         .outputMode("append")
//        .outputMode("complete")
//         .format("console")
//        .option("truncate", false)
        .format("csv")
        .option("checkpointLocation", "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/checkpoint")
        .option("path", "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
