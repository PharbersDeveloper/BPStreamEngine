package com.pharbers.StreamEngine.Utils.Strategy.JobStrategy

import java.util
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

import collection.JavaConverters._

/** 功能描述
 *
 * @author dcs
 * @version 0.0
 * @since 2020/02/04 14:02
 * @note 一些值得注意的地方
 */

//todo：因为ConfigDef的原因，很难做到无状态
case class BPSCommonJobStrategy(config: Map[String, String], inoutConfigDef: ConfigDef = new ConfigDef()) extends BPStrategyComponent {
    final private val LISTEN_EVENTS_KEY = "listens"
    final private val LISTEN_EVENTS_DOC = "listener hit event type"
    final private val LISTEN_EVENTS_DEFAULT = new util.ArrayList[String]()

    val jobIdConfigStrategy = new BPSJobIdConfigStrategy(config, configDef)
    val jobConfig = BPSConfig(configDef, config)

    override def createConfigDef(): ConfigDef = inoutConfigDef
            .define(LISTEN_EVENTS_KEY, Type.LIST, LISTEN_EVENTS_DEFAULT, Importance.HIGH, LISTEN_EVENTS_DOC)

    def getJobConfig: BPSConfig = jobConfig

    def getRunId: String = jobIdConfigStrategy.getRunId

    def getJobId: String = jobIdConfigStrategy.getJobId

    def getListens: Seq[String] = jobConfig.getList(LISTEN_EVENTS_KEY).asScala

    def getKafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]

    def getSpark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    def getHdfsFile: BPSHDFSFile = BPSConcertEntry.queryComponentWithId("hdfs").asInstanceOf[BPSHDFSFile]

    def pushMsg(msg: BPSEvents, isLocal: Boolean): Unit ={
        if (isLocal){
            BPSLocalChannel.offer(msg)
        } else {
            getKafka.callKafka(msg)
        }
    }

    override val componentProperty: Component2.BPComponentConfig = null
    override val strategyName: String = "common strategy"
}
