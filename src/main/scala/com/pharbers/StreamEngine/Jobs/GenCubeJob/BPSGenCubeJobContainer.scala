package com.pharbers.StreamEngine.Jobs.GenCubeJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.GenCubeJob.Listener.GenCubeJobStartListener
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Strategy.BPSKfkBaseStrategy
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.GenCubeJobSubmit
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

object BPSGenCubeJobContainer {
    def apply(strategy: BPSKfkBaseStrategy,
              spark: SparkSession,
              config: Map[String, String]): BPSGenCubeJobContainer =
        new BPSGenCubeJobContainer(spark, config)
}

/** 执行 GenCube 的 Job
  *
  * @author jeorch
  * @version 0.0.1
  * @since 2020/3/19 15:43
  */
class BPSGenCubeJobContainer(override val spark: SparkSession,
                             config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob {

    override val strategy: BPSKfkBaseStrategy = null
    type T = BPSKfkBaseStrategy

    var metadata: Map[String, Any] = Map.empty

    final val DEFAULT_LISTENING_TOPIC = "GenCubeJobSubmit"

    final val CONTAINER_ID_KEY = "container.id"
    final val CONTAINER_ID_DOC = "The value is container's id."

    final val CONTAINER_LISTENING_TOPIC_KEY = "container.listening.topic"
    final val CONTAINER_LISTENING_TOPIC_DOC = "The value is a topic which is this container listened to and send job request."

    val configDef: ConfigDef = new ConfigDef()
        .define(CONTAINER_ID_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, CONTAINER_ID_DOC)
        .define(CONTAINER_LISTENING_TOPIC_KEY, Type.STRING, DEFAULT_LISTENING_TOPIC, Importance.HIGH, CONTAINER_LISTENING_TOPIC_DOC)
    private val jobConfig: BPSConfig = BPSConfig(configDef, config)

    // container id 作为 runner id
    val id: String = jobConfig.getString(CONTAINER_ID_KEY)
    val listeningTopic: String = jobConfig.getString(CONTAINER_LISTENING_TOPIC_KEY)
    var pkc: PharbersKafkaConsumer[String, GenCubeJobSubmit] = null

    override def open(): Unit = {
        logger.info("gen-cube job container open with runner-id ========>" + id)
        //注册container后，使用kafka-consumer监听具体job的启动
        pkc = new PharbersKafkaConsumer[String, GenCubeJobSubmit](
            listeningTopic :: Nil,
            1000,
            Int.MaxValue, GenCubeJobStartListener(id, spark, this).process
        )

    }

    override def exec(): Unit = {
        ThreadExecutor().execute(pkc)
    }

    override def close(): Unit = {
        super.close()
        pkc.close()
        logger.info("gen-cube job container closed with runner-id ========>" + id)
    }

    override def handlerExec(handler: BPSEventHandler): Unit = {}

    override def registerListeners(listener: BPStreamListener): Unit = {}
}
