package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.BPSOssPartitionJob
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Jobs.KfkSinkJob.KafkaOutputJob
import com.pharbers.StreamEngine.Utils.Config.KafkaConfig
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions.mapAsScalaMap

object BPSOssPartitionJobContainer {
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSOssPartitionJobContainer = new BPSOssPartitionJobContainer(strategy, spark)
}

class BPSOssPartitionJobContainer(override val strategy: BPSKfkJobStrategy, val spark: SparkSession) extends BPSJobContainer {
    val id = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy
    import spark.implicits._
    //    val listener = new BPSOssListener(spark, this)

    override def open(): Unit = {
        val reading = spark.readStream
            .format("kafka")
            .options(mapAsScalaMap(KafkaConfig.PROPS).map(x => (x._1.toString, x._2.toString)))
//            .option("kafka.bootstrap.servers", "123.56.179.133:9092")
//            .option("kafka.security.protocol", "SSL")
//            .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
//            .option("kafka.ssl.keystore.password", "pharbers")
//            .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
//            .option("kafka.ssl.truststore.password", "pharbers")
//            .option("kafka.ssl.endpoint.identification.algorithm", " ")
//            .option("startingOffsets", "earliest")
            .option("subscribe", strategy.getTopic)
            .load()

        inputStream = Some(reading
            .selectExpr(
                """deserialize(value) AS value""",
                "timestamp"
            ).toDF()
            .withWatermark("timestamp", "24 hours")
            .select(
                from_json($"value", strategy.getSchema).as("data"), col("timestamp")
            ).select("data.*", "timestamp"))
    }

    override def exec(): Unit = inputStream match {
        case Some(is) => {
            val listener = new BPSOssListener(spark, this)
            listener.active(is)

//            val outputJob = new KafkaOutputJob
//            outputJob.sink(is.selectExpr("""data AS value""", "jobId", "traceId", "type"))

            listeners = listener :: listeners

            is.filter($"type" === "SandBox").writeStream
                .partitionBy("jobId")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "/workData/streamingV2/" + this.id + "/checkpoint")
                .option("path", "/workData/streamingV2/" + this.id + "/files")
                .start()
        }
        case None => ???
    }

    override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
        jobs.get(id) match {
            case Some(job) => job
            case None => {
                val job = BPSOssPartitionJob(id, spark, this.inputStream, this)
                job.exec()
                jobs += id -> job
                job
            }
        }
    }
}
