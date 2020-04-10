package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.BPSOssPartitionJob
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Strategy.BPSKfkBaseStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession._


object BPSOssPartitionJobContainer {
    def apply(strategy: BPSKfkBaseStrategy, spark: SparkSession): BPSOssPartitionJobContainer =
        new BPSOssPartitionJobContainer(null)

    def apply(componentProperty: Component2.BPComponentConfig): BPSOssPartitionJobContainer =
        new BPSOssPartitionJobContainer(componentProperty)
}

@Component(name = "BPSOssPartitionJobContainer", `type` = "BPSOssPartitionJobContainer")
class BPSOssPartitionJobContainer(override val componentProperty: Component2.BPComponentConfig)
    extends BPSJobContainer with BPDynamicStreamJob{

    // TODO: Stream Job 下移
    val id: String = componentProperty.id
    val jobId: String = UUID.randomUUID().toString
    val description: String = "InputStream"

    type T = BPKafkaSession
    val strategy: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]
    override val spark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
    import spark.implicits._

    override def open(): Unit = {
        val reading = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "123.56.179.133:9092")
            .option("kafka.security.protocol", "SSL")
            .option("kafka.ssl.keystore.location", "./kafka.broker1.keystore.jks")
            .option("kafka.ssl.keystore.password", "pharbers")
            .option("kafka.ssl.truststore.location", "./kafka.broker1.truststore.jks")
            .option("kafka.ssl.truststore.password", "pharbers")
            .option("kafka.ssl.endpoint.identification.algorithm", " ")
            .option("maxOffsetsPerTrigger", 100000)
            .option("startingOffsets", "earliest")
            .option("subscribe", s"${strategy.getDataTopic}, ${strategy.getMsgTopic}")
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

    override def exec(): Unit = {
        val listener = BPJobRemoteListener[BPSComponentConfig](this, List("SandBox-Start"))(x => starJob(x))
        listener.active(null)
        listeners = listener +: listeners
    }

    override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
        jobs.get(id) match {
            case Some(job) => job
            case None =>
                val job = category match {
                    case _ => ???
                }
                jobs += id -> job
                job
        }
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}
    override def handlerExec(handler: BPSEventHandler): Unit = {}

    override def createConfigDef(): ConfigDef = ???

    def starJob(event: BPSTypeEvents[BPSComponentConfig]): Unit ={
        val job = BPSOssPartitionJob(this, event.date)
        jobs += job.id -> job
        job.open()
        job.exec()
    }

}
