package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer

import java.util.UUID
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.pharbers.StreamEngine.Jobs.SqlTableJob.BPSqlTableJob
import com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableListener.{BPSqlTableKafkaListener, BPStreamOverListener}
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Config.BPSConfig
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor.executorService
import com.pharbers.kafka.schema.HiveTask
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 10:57
  * @note 一些值得注意的地方
  */
class BPSqlTableJobContainer(val spark: SparkSession, config: Map[String, String]) extends BPSJobContainer with BPDynamicStreamJob{

    override type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null
    final val TOPIC_CONFIG_KEY = "topic"
    final val TOPIC_CONFIG_DOC = "kafka topic"
    final val RUN_ID_CONFIG_KEY = "runId"
    final val RUN_ID_CONFIG_DOC = "run id"
    final val VERSIONS_CONFIG_KEY = "version"
    final val VERSIONS_CONFIG_DOC = "version"
    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = new ConfigDef()
            .define(TOPIC_CONFIG_KEY, Type.STRING, "HiveTask", Importance.HIGH, TOPIC_CONFIG_DOC)
            .define(RUN_ID_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, RUN_ID_CONFIG_DOC)
            .define(VERSIONS_CONFIG_KEY, Type.STRING, UUID.randomUUID().toString, Importance.HIGH, VERSIONS_CONFIG_DOC)
    private val jobConfig: BPSConfig = BPSConfig(configDef, config)

    val runId: String = jobConfig.getString(RUN_ID_CONFIG_KEY)
    val jobId: String = UUID.randomUUID().toString
    val id: String = runId
    val description: String = "sql_table"

    val executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
    //todo: 配置文件
    val tableNameMap: Map[String, String] = Map(
        "CPA&GYC" -> "cpa",
        "CHC" -> "chc",
        "RESULT" -> "result",
        "PROD" -> "prod"
    )

    var jobConfigs:  Map[String,  Map[String, String]] = Map()


    override def open(): Unit = {
        logger.info("open BPSqlTableJobContainer")
    }

    override def exec(): Unit ={
        logger.info("开启BPSqlTableKafkaListener")
        val listener = BPSqlTableKafkaListener(this, jobConfig.getString(TOPIC_CONFIG_KEY))
        listener.active(null)
        listeners = listeners :+ listener
    }

    override def close(): Unit = {
        super.close()
        jobs.foreach(x => x._2.close())
    }

    def runJob(): Unit ={
        val configs = jobConfigs
        jobConfigs = Map()
        configs.values.foreach(x =>{
            val job = new BPSqlTableJob(this, spark, x)
            executorService.execute(new Runnable{
                override def run(): Unit = {
                    job.open()
                    job.exec()
                }
            })
            jobs = jobs ++ Map(job.id -> job)
        })
    }

    def addJobConfig(config: Map[String, String]): Unit ={
        val providers = config.getOrElse("providers", "").split(",")
        providers.toSet.intersect(tableNameMap.keySet).foreach(key => {
            val tableName = tableNameMap(key)
            val (urls, dataSets) = if(jobConfigs.contains(tableName)){
                (jobConfigs(tableName)("urls") + "," + config("url"), jobConfigs(tableName)("dataSets") + "," + config("datasetId"))
            } else {
                (config("url"),config("datasetId") )
            }
            jobConfigs = jobConfigs ++ Map(tableName -> (config ++ Map("tableName" -> tableName, "urls" -> urls, "dataSets" -> dataSets, "version" -> "")))
        })
    }

    def hiveTaskHandler(msg: HiveTask): Unit ={
        val url = msg.getUrl.toString
        val path =if(msg.getTaskType.toString != "end"){
            if(url.contains("contents")) {
                url.substring(0, url.lastIndexOf("contents"))
            } else {
                logger.debug(url)
                "/"
            }
        } else {
            "/"
        }

        val listenerConfig = Map(
            "runId" -> runId,
            "jobId" -> UUID.randomUUID().toString,
            "url" -> msg.getUrl.toString,
            "length" -> msg.getLength.toString,
            "rowRecordPath" -> (path + "row_record"),
            "metadataPath" -> (path +  "metadata"),
            "errorPath" -> (path +  "err"),
            "taskType" -> msg.getTaskType.toString,
            "datasetId" -> msg.getDatasetId.toString
        )
        val listener = BPStreamOverListener(this, listenerConfig)
        listener.active(null)
        listeners = listeners :+ listener
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}

