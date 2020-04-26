package com.pharbers.StreamEngine.Utils.Strategy.JobStrategy

import java.util
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

/** 功能描述
 *
 * @param args 构造参数
 * @tparam T 构造泛型参数
 * @author dcs
 * @version 0.0
 * @since 2020/02/04 13:49
 * @note 一些值得注意的地方
 */
//todo：因为ConfigDef的原因，很难做到无状态
class BPSJobIdConfigStrategy(override val componentProperty: Component2.BPComponentConfig, inoutConfigDef: ConfigDef = new ConfigDef()) extends BPStrategyComponent{
    final val JOB_ID_CONFIG_KEY = "jobId"
    final val JOB_ID_CONFIG_DOC = "job id"
    final val TRACE_ID_CONFIG_KEY = "traceId"
    final val TRACE_ID_CONFIG_DOC = "trace id"
    final val JOB_ID_CONFIG_DEFAULT = UUID.randomUUID().toString
    final val TRACE_ID_CONFIG_DEFAULT = ""


    override def createConfigDef(): ConfigDef = inoutConfigDef
            .define(JOB_ID_CONFIG_KEY, Type.STRING, JOB_ID_CONFIG_DEFAULT, Importance.HIGH, JOB_ID_CONFIG_DOC)
            .define(TRACE_ID_CONFIG_KEY,Type.STRING,TRACE_ID_CONFIG_DEFAULT, Importance.HIGH, JOB_ID_CONFIG_DOC)
    val jobConfig = BPSConfig(configDef, componentProperty.config)

    //id是BPStreamJob实例唯一标识
    def getId: String = componentProperty.id

    //用户一次操作产生一个
    def getTraceId: String = jobConfig.getString(TRACE_ID_CONFIG_KEY)

    //一个BPStream进程使用同一个runId
    def getRunId: String = BPSConcertEntry.runner_id

    //jobId是job中的抽象概念，可以用来标识job的一个任务，所以一个job可以有多个jobId
    def getJobId: String = jobConfig.getString(JOB_ID_CONFIG_KEY)

    override val strategyName: String = "id strategy"
}
