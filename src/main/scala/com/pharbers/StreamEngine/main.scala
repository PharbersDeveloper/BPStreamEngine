
import java.util.UUID

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json

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

    val logsDf = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "123.56.179.133:9092")
        .option("kafka.security.protocol", "SSL")
        .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
        .option("kafka.ssl.keystore.password", "pharbers")
        .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
        .option("kafka.ssl.truststore.password", "pharbers")
        .option("kafka.ssl.endpoint.identification.algorithm", " ")
        .option("subscribe", "test001")
        .option("startingOffsets", "earliest")
        .load()

    val selectDf = logsDf.selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "timestamp").as[(String, String, String)].toDF()
        .withWatermark("timestamp", "24 hours")
        .select(
            from_json($"value", schema).as("data")
        ).select("data.*")

    val jobId = UUID.randomUUID()
    val query = selectDf.writeStream
         .outputMode("append")
//         .format("console")
        .format("parquet")
        .option("checkpointLocation", "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/checkpoint")
        .option("path", "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
