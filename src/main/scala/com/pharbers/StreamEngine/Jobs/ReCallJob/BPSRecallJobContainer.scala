package com.pharbers.StreamEngine.Jobs.ReCallJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.ReCallJob.BPSRecallJobContainer._
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/12 12:12
  * @note 一些值得注意的地方
  */
case class BPSRecallJobContainer(config: Map[String, String]) extends BPSJobContainer with BPDynamicStreamJob {

    override type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null
    override val spark: SparkSession = null

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???

    val jobConfig: BPSConfig = BPSConfig(configDef, config)
    val runId: String = jobConfig.getString(RUN_ID_CONFIG_KEY)
    val jobId: String = UUID.randomUUID().toString
    val id: String = runId

    override def open(): Unit = super.open()

    override def exec(): Unit = {
        val listener = ReCallJobListener(this, jobConfig.getString(TOPIC_CONFIG_KEY), runId, jobId)
        listener.active(null)
        listeners = listeners :+ listener
    }

    override def close(): Unit = {
        logger.info(s"close BPSRecallJobContainer, config: $config")
        super.close()
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}

object BPSRecallJobContainer {
    val TOPIC_CONFIG_KEY = "topic"
    val TOPIC_CONFIG_DOC = "kafka topic"
    val RUN_ID_CONFIG_KEY = "runId"
    val RUN_ID_CONFIG_DOC = "run id"
    val configDef: ConfigDef = new ConfigDef()
            .define(TOPIC_CONFIG_KEY, Type.STRING, "HiveTask", Importance.HIGH, TOPIC_CONFIG_DOC)
            .define(RUN_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, RUN_ID_CONFIG_DOC)
}
