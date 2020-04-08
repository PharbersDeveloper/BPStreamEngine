package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.BPSOssPartitionJob
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSComponentConfig
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Strategy.BPSKfkBaseStrategy
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object BPSOssPartitionJobContainer {
    def apply(strategy: BPSKfkBaseStrategy, spark: SparkSession): BPSOssPartitionJobContainer =
        new BPSOssPartitionJobContainer(null)

    def apply(componentProperty: Component2.BPComponentConfig): BPSOssPartitionJobContainer =
        new BPSOssPartitionJobContainer(componentProperty)
}

@Component(name = "BPSOssPartitionJobContainer", `type` = "BPSOssPartitionJobContainer")
class BPSOssPartitionJobContainer(override val componentProperty: Component2.BPComponentConfig)
        extends BPSJobContainer with BPDynamicStreamJob {

    final private val FILE_MSG_TYPE_KEY = "FileMetaData.msgType"
    final private val FILE_MSG_TYPE_DOC = "push FileMetaData msg type"
    final private val FILE_MSG_TYPE_DEFAULT = "FileMetaData.msgType"

    val description: String = "InputStream"
    type T = BPSCommonJobStrategy
    val strategy = BPSCommonJobStrategy(componentProperty.config, configDef)
    val id: String = componentProperty.id
    val jobId: String = strategy.getJobId
    override val spark: SparkSession = strategy.getSpark

    import spark.implicits._

    override def open(): Unit = {
        val kafkaSession: BPKafkaSession = strategy.getKafka
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
                .option("subscribe", s"${kafkaSession.getDataTopic}, ${kafkaSession.getMsgTopic}")
                .load()

        // TODO: 求稳定，机器不够，切记
        inputStream = Some(reading
                .selectExpr(
                    """deserialize(value) AS value""",
                    "timestamp"
                ).toDF()
                .withWatermark("timestamp", "24 hours")
                .select(
                    from_json($"value", kafkaSession.getSchema).as("data"), col("timestamp")
                ).select("data.*", "timestamp"))
    }

    override def exec(): Unit = {
        val listenEvent = strategy.getListens
        val listener = BPJobRemoteListener[Map[String, String]](this, listenEvent.toList)(x => starJob(x))
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

    override def createConfigDef(): ConfigDef = {
        new ConfigDef().define(FILE_MSG_TYPE_KEY, Type.STRING, FILE_MSG_TYPE_DEFAULT, Importance.HIGH, FILE_MSG_TYPE_DOC)
    }

    def starJob(event: BPSTypeEvents[Map[String, String]]): Unit = {
        val job = BPSOssPartitionJob(this, BPSComponentConfig(UUID.randomUUID().toString, "BPSOssPartitionJob", Nil, event.date))
        jobs += job.id -> job
        job.open()
        job.exec()
        val lengthMsgType: String = strategy.jobConfig.getString(FILE_MSG_TYPE_KEY)
        val ossMetadataListener = BPSOssListener(job, lengthMsgType)
        ossMetadataListener.active(null)
        listeners = listeners :: ossMetadataListener
    }
}
