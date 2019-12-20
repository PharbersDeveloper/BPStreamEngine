package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.BPSOssPartitionJob
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions.mapAsScalaMap

object BPSOssPartitionJobContainer {
    def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSOssPartitionJobContainer =
        new BPSOssPartitionJobContainer(strategy, spark, Map.empty)
}

class BPSOssPartitionJobContainer(override val strategy: BPSKfkJobStrategy, val spark: SparkSession, config: Map[String, String]) extends BPSJobContainer with BPDynamicStreamJob{
    val id: String = UUID.randomUUID().toString
    val jobId = UUID.randomUUID().toString
    type T = BPSKfkJobStrategy
    import spark.implicits._

    override def open(): Unit = {
        val reading = spark.readStream
            .format("kafka")
//            .options(mapAsScalaMap(KafkaConfig.PROPS).map(x => (x._1.toString, x._2.toString)))
            .option("kafka.bootstrap.servers", "123.56.179.133:9092")
            .option("kafka.security.protocol", "SSL")
            .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
            .option("kafka.ssl.keystore.password", "pharbers")
            .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
            .option("kafka.ssl.truststore.password", "pharbers")
            .option("kafka.ssl.endpoint.identification.algorithm", " ")
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 10000)
//            .option("startingOffsets", "latest")
            .option("subscribe", strategy.getTopic)
//            .option("failOnDataLoss", "false")
            .load()

        // TODO: 求稳定，机器不够，切记
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
            val listener = BPSOssListener(spark, this, jobId)
            listener.active(is)

            listeners = listener :: listeners

            is.filter($"type" === "SandBox").writeStream
                .partitionBy("jobId")
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "/jobs/" +  this.id + "/checkpoint")
                .option("path", "/jobs/" + this.id + "/" + jobId +  "/contents")
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

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
