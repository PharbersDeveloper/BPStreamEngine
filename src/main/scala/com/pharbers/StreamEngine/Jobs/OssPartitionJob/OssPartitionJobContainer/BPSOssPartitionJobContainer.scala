package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.{BPSKafkaMsgJob, BPSOssPartitionJob}
import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener.BPSOssListener
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSComponentConfig
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
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
        extends BPSJobContainer {

    final private val FILE_MSG_TYPE_KEY = "FileMetaData.msgType"
    final private val FILE_MSG_TYPE_DOC = "push FileMetaData msg type"
    final private val FILE_MSG_TYPE_DEFAULT = "FileMetaData.msgType"
    final private val STARTING_OFFSETS_KEY = "starting.offsets"
    final private val STARTING_OFFSETS_DOC = "kafka offsets begin"
    final private val STARTING_OFFSETS_DEFAULT = "earliest" //可以分别指定{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}} -2 = earliest， -1 = latest

    val description: String = "InputStream"
    type T = BPSCommonJobStrategy
    val strategy = BPSCommonJobStrategy(componentProperty, configDef)
    val id: String = strategy.getId
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
                .option("startingOffsets", strategy.jobConfig.getString(STARTING_OFFSETS_KEY))
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
        val msgJob = new BPSKafkaMsgJob(this, BPSComponentConfig(UUID.randomUUID().toString, "BPSOssPartitionJob", Nil, Map()))
        msgJob.open()
        msgJob.exec()
        jobs += msgJob.id -> msgJob
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

    override def createConfigDef(): ConfigDef = {
        new ConfigDef()
                .define(FILE_MSG_TYPE_KEY, Type.STRING, FILE_MSG_TYPE_DEFAULT, Importance.HIGH, FILE_MSG_TYPE_DOC)
                .define(STARTING_OFFSETS_KEY, Type.STRING, STARTING_OFFSETS_DEFAULT, Importance.HIGH, STARTING_OFFSETS_DOC)
    }

    def starJob(event: BPSTypeEvents[Map[String, String]]): Unit = {
        val job = BPSOssPartitionJob(this, BPSComponentConfig(UUID.randomUUID().toString, "BPSKafkaMsgJob", Nil, event.date))
        jobs += job.id -> job
        job.open()
        job.exec()
        val lengthMsgType: String = strategy.jobConfig.getString(FILE_MSG_TYPE_KEY)
        val ossMetadataListener = BPSOssListener(job, lengthMsgType)
        ossMetadataListener.active(null)
        listeners = listeners :+ ossMetadataListener
    }
}
