package com.pharbers.StreamEngine.Jobs.DataCleanJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.DataCleanJob.EditDistanceJob.{BPEditDistance, BPEditDistanceV2Func}
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSComponentConfig
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobLocalListener, BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Event.msgMode.DataCleanTask
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/05/29 11:32
  * @note 一些值得注意的地方
  */
@Component(name = "BPDataCleanJobContainer", `type` = "BPDataCleanJobContainer")
class BPDataCleanJobContainer(override val componentProperty: Component2.BPComponentConfig) extends BPSJobContainer with BPDynamicStreamJob {
    override def registerListeners(listener: BPStreamListener): Unit = ???

    override def handlerExec(handler: BPSEventHandler): Unit = ???

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty, configDef)
    override val id: String = strategy.getId
    override val description: String = "BPDataCleanJobContainer"
    override val spark: SparkSession = strategy.getSpark


    override def createConfigDef(): ConfigDef = new ConfigDef()

    override def open(): Unit = logger.info("BPDataCleanJobContainer open")

    override def exec(): Unit = {
        val jobStartListener = BPJobRemoteListener[DataCleanTask](this, strategy.getListens.toList)(startJob)
        jobStartListener.active(null)
        listeners = listeners :+ jobStartListener
    }

    protected def startJob(msg: BPSTypeEvents[DataCleanTask]): Unit ={
        inputStream = Some(spark.sql(s"select * from ${msg.data.tableName}"))
        val config = Map("jobId" -> msg.jobId, BPEditDistanceV2Func.TABLE_NAME_CONFIG_KEY -> msg.data.tableName)
        val editDistanceId = UUID.randomUUID().toString
        val editDistanceJob = new BPEditDistanceV2Func(this, BPSComponentConfig(editDistanceId, s"BPEditDistance_$editDistanceId", Nil, config))
        editDistanceJob.open()
        editDistanceJob.exec()
        jobs += editDistanceId -> editDistanceJob
    }
}

object BPDataCleanJobContainer {
    def apply(componentProperty: Component2.BPComponentConfig): BPDataCleanJobContainer = {
        new BPDataCleanJobContainer(componentProperty)
    }
}
