package com.pharbers.StreamEngine.Jobs.EsJob.EsJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.EsJob.Listener.EsSinkJobStartListener
import com.pharbers.StreamEngine.Utils.Component2
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.Strategy.BPSKfkBaseStrategy
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.EsSinkJobSubmit
import org.apache.kafka.common.config.ConfigDef

object BPSEsSinkJobContainer {
    def apply(strategy: BPSKfkBaseStrategy,
              spark: SparkSession,
              config: Map[String, String]): BPSEsSinkJobContainer =
        new BPSEsSinkJobContainer(spark, config)
}

/** 执行 EsSink 的 Job
 *
 * @author jeorch
 * @version 0.1
 * @since 2019/11/19 15:43
 * @node 可用的配置参数
 */
class BPSEsSinkJobContainer(override val spark: SparkSession,
                            config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob {

    override val strategy: BPSKfkBaseStrategy = null
    type T = BPSKfkBaseStrategy

    var metadata: Map[String, Any] = Map.empty
    final val DEFAULT_LISTENING_TOPIC = "EsSinkJobSubmit"

    // container id 作为 runner id
    val id: String = config.getOrElse("id", UUID.randomUUID().toString)
    val listeningTopic: String = config.getOrElse("listeningTopic", DEFAULT_LISTENING_TOPIC)
    var pkc: PharbersKafkaConsumer[String, EsSinkJobSubmit] = null

    override def open(): Unit = {
        logger.info("es sink job container open with runner-id ========>" + id)
        //注册container后，使用kafka-consumer监听具体job的启动
        pkc = new PharbersKafkaConsumer[String, EsSinkJobSubmit](
            listeningTopic :: Nil,
            1000,
            Int.MaxValue, EsSinkJobStartListener(id, spark, this).process
        )

    }

    override def exec(): Unit = {
        ThreadExecutor().execute(pkc)
    }

    override def close(): Unit = {
        super.close()
        pkc.close()
        logger.info("es sink job container closed with runner-id ========>" + id)
    }

    override def handlerExec(handler: BPSEventHandler): Unit = {}

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???

    override val description: String = "es_sink_job"
}
