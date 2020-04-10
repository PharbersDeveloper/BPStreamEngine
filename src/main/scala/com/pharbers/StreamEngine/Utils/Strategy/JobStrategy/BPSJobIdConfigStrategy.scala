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
class BPSJobIdConfigStrategy(config: Map[String, String], inoutConfigDef: ConfigDef = new ConfigDef()) extends BPStrategyComponent{
    final private val JOB_ID_CONFIG_KEY = "jobId"
    final private val JOB_ID_CONFIG_DOC = "job id"
    final private val JOB_ID_CONFIG_DEFAULT = UUID.randomUUID().toString


    override def createConfigDef(): ConfigDef = inoutConfigDef
            .define(JOB_ID_CONFIG_KEY, Type.STRING, JOB_ID_CONFIG_DEFAULT, Importance.HIGH, JOB_ID_CONFIG_DOC)
    val jobConfig = BPSConfig(configDef, config)


    def getRunId: String = BPSConcertEntry.runner_id

    def getJobId: String = jobConfig.getString(JOB_ID_CONFIG_KEY)

    override val strategyName: String = "id strategy"
    override val componentProperty: Component2.BPComponentConfig = null
}
