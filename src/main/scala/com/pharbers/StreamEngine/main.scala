
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.AvroDeserializer.AvroDeserializer
import com.pharbers.StreamEngine.schemaReg.SchemaReg
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

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

    val dataDf = selectDf.filter($"jobId" === "test09241724")

    val dataSchemaDf = dataDf.filter($"type" === "SandBox-Schema").writeStream
        .foreach(
            new ForeachWriter[Row] {
                def open(partitionId: Long, version: Long): Boolean = true

                def process(value: Row) : Unit = {
                    import org.apache.hadoop.fs.FSDataOutputStream
                    import org.apache.hadoop.fs.FileSystem
                    import java.io.BufferedWriter
                    import java.io.OutputStreamWriter
                    import java.nio.charset.StandardCharsets
                    val configuration: Configuration = new Configuration
                    configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
                    val fileSystem: FileSystem = FileSystem.get(configuration)
                    //Create a path
                    val fileName: String = "tmp.txt"
                    val hdfsWritePath: Path = new Path("/test/streaming/" + fileName)
                    val fsDataOutputStream: FSDataOutputStream =
                        if (fileSystem.exists(hdfsWritePath))
                            fileSystem.append(hdfsWritePath)
                        else
                            fileSystem.create(hdfsWritePath)

                    val bufferedWriter: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))
                    bufferedWriter.write("Java API to append data in HDFS file")
                    bufferedWriter.newLine()
                    bufferedWriter.write(value.getAs[String]("data"))
                    bufferedWriter.newLine()
                    bufferedWriter.close()

//                    println(spark)
//                    println(spark.sqlContext)
//                    println(value)
//                    val t = new java.util.LinkedList[Row]()
//                    t.add(Row.apply("abcde"))
//                    val tmp = spark.sqlContext.createDataFrame(t, SchemaReg.tmpSche)
//                    tmp.write.mode("append").csv("hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/tmp")
                }

                def close(errorOrNull: scala.Throwable): Unit = {

                }
            }
        ).start()

    val jobId = UUID.randomUUID()
    val path = "hdfs://192.168.100.137:9000/test/streaming/" + jobId + "/files"

    val query = selectDf.writeStream
         .outputMode("append")
//        .outputMode("complete")
         .format("console")
//        .format("csv")
        .option("checkpointLocation", "/test/streaming/" + jobId + "/checkpoint")
        .option("path", "/test/streaming/" + jobId + "/files")
        .start()

    query.awaitTermination()
}
