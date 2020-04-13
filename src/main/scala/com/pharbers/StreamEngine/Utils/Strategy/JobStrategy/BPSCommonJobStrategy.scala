package com.pharbers.StreamEngine.Utils.Strategy.JobStrategy

import java.util
import java.util.UUID

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
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
object BPSCommonJobStrategy {
    def apply(componentProperty: Component2.BPComponentConfig, inoutConfigDef: ConfigDef): BPSCommonJobStrategy =
        new BPSCommonJobStrategy(componentProperty, inoutConfigDef)

    @deprecated
    def apply(inputConfig: Map[String, String], inoutConfigDef: ConfigDef = new ConfigDef()): BPSCommonJobStrategy =
        new BPSOldJobStrategy(inputConfig, inoutConfigDef)
}

//todo：因为ConfigDef的原因，很难做到无状态
class BPSCommonJobStrategy(override val componentProperty: Component2.BPComponentConfig, inputConfigDef: ConfigDef = new ConfigDef())
        extends BPStrategyComponent {

    override val strategyName: String = "common strategy"

    final val LISTEN_EVENTS_KEY = "listens"
    final val LISTEN_EVENTS_DOC = "listener hit event type"
    final val LISTEN_EVENTS_DEFAULT = new util.ArrayList[String]()

    final val JOB_STATUS_EVENT_TYPE = "job-status"

    val jobIdConfigStrategy = new BPSJobIdConfigStrategy(componentProperty, configDef)
    val jobConfig = BPSConfig(configDef, componentProperty.config)

    override def createConfigDef(): ConfigDef = inputConfigDef
            .define(LISTEN_EVENTS_KEY, Type.LIST, LISTEN_EVENTS_DEFAULT, Importance.HIGH, LISTEN_EVENTS_DOC)

    def getJobConfig: BPSConfig = jobConfig

    //一个BPStream进程使用同一个runId
    def getRunId: String = jobIdConfigStrategy.getRunId

    //id是BPStreamJob实例唯一标识
    def getId: String = jobIdConfigStrategy.getId

    //jobId是job中的抽象概念，可以用来标识job的一个任务，所以一个job可以有多个jobId
    def getJobId: String = jobIdConfigStrategy.getJobId

    def getListens: Seq[String] = jobConfig.getList(LISTEN_EVENTS_KEY).asScala

    def getKafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]

    def getSpark: SparkSession = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]

    def getHdfsFile: BPSHDFSFile = BPSConcertEntry.queryComponentWithId("hdfs").get.asInstanceOf[BPSHDFSFile]

    def getParseSchema: BPSParseSchema = BPSConcertEntry.queryComponentWithId("parse schema").get.asInstanceOf[BPSParseSchema]

    def pushMsg(msg: BPSEvents, isLocal: Boolean): Unit ={
        if (isLocal){
            BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel].offer(msg)
        } else {
            getKafka.callKafka(msg)
        }
    }
}

@deprecated
//使用BPSCommonJobStrategy,这儿为了兼容之前版本，并且不让调用getId出现奇怪的问题
class BPSOldJobStrategy(inputConfig: Map[String, String], inoutConfigDef: ConfigDef = new ConfigDef())
        extends BPSCommonJobStrategy(BPSComponentConfig(UUID.randomUUID().toString, "", Nil, inputConfig), inoutConfigDef){
    @deprecated
    //使用BPSCommonJobStrategy.getId
    override def getId: String = throw new Error("已废弃")
}
