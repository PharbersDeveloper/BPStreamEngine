package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SqlTableJob.BPSqlTableJob
import com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableListener.BPStreamOverCheckJob
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobLocalListener, BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Job.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.kafka.common.config.ConfigDef
import org.apache.spark.sql.SparkSession

/** 功能描述
  *
  * @author dcs
  * @version 0.0
  * @since 2019/12/11 10:57
  * @note 一些值得注意的地方
  */
object BPSqlTableJobContainer {
    def apply(componentProperty: Component2.BPComponentConfig): BPSqlTableJobContainer =
        new BPSqlTableJobContainer(componentProperty)
}

@Component(name = "BPSqlTableJobContainer", `type` = "BPSqlTableJobContainer")
class BPSqlTableJobContainer(override val componentProperty: Component2.BPComponentConfig) extends BPSJobContainer with BPDynamicStreamJob {

    final val CHECK_EVENT_TYPE = "hive-check"

    override type T = BPSCommonJobStrategy
    override val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty, configDef)

    override def createConfigDef(): ConfigDef = new ConfigDef()
    override val spark: SparkSession = strategy.getSpark

    override val jobId: String = strategy.getJobId
    val id: String = strategy.getId
    val description: String = "BPSSqlTableJob"

    //todo: 配置文件
    val tableNameMap: Map[String, String] = Map(
        "CPA&GYC" -> "cpa",
        "CHC" -> "chc",
        "RESULT" -> "result",
        "PROD" -> "prod",
        "TEST" -> "test"
    )

    var jobConfigs: Map[TaskKey, List[HiveTask]] = Map()
//    var tasks: Map[String, HiveTask] = Map()

    override def open(): Unit = {
        logger.info("open BPSqlTableJobContainer")
    }

    override def exec(): Unit = {
        logger.info("开启BPSqlTableKafkaListener")
        //        val listener = BPSqlTableKafkaListener(this, jobConfig.getString(TOPIC_CONFIG_KEY))
        val hiveTaskListener = BPJobRemoteListener[HiveTask](this, strategy.getListens.toList)(hiveTaskHandle)
        hiveTaskListener.active(null)
//        val checkListener = BPJobLocalListener[List[String]](this, List(CHECK_EVENT_TYPE))(x => addJobConfig(x.jobId, x.data))
//        checkListener.active(null)
        val jobFinishListener = BPJobLocalListener[Map[String, String]](this, List(strategy.JOB_STATUS_EVENT_TYPE))(x =>
            if(x.data.values.head == BPSJobStatus.Success.toString) finishJobWithId(x.data.keySet.head))
        jobFinishListener.active(null)
        listeners = listeners :+ hiveTaskListener :+
//                checkListener :+
                jobFinishListener
    }

    override def close(): Unit = {
        super.close()
        jobs.foreach(x => x._2.close())
    }

    override def finishJobWithId(id: String): Unit = {
        super.finishJobWithId(id)
        if(jobs.contains(id)) jobs(id).close()
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}

    private def runJob(endTask: BPSTypeEvents[HiveTask]): Unit = {
//        var checkCount = 0
//        while (tasks.nonEmpty && checkCount < 20 && jobs.nonEmpty) {
//            Thread.sleep(500)
//            checkCount += 1
//        }
//        if (checkCount >= 20) {
//            logger.warn(s"有check job未完成就开始了聚合hive， jos：${tasks.values.map(x => x.toString).mkString(",")}")
//        }
        val configs = jobConfigs.filter(x => x._1.jobId == endTask.jobId)
        jobConfigs = jobConfigs.filterNot(x => x._1.jobId == endTask.jobId)
        val sqlId = UUID.randomUUID().toString
        configs.foreach { case (taskKey, task) =>
            val (urls, taskType, errPaths, dataSets) = task.map(x => (x.url, x.taskType, x.errPath, x.datasetId)).reduce((l, r) => {
                ( s"${l._1},${r._1}", l._2, s"${l._3},${r._3}", s"${l._4},${r._4}")
            })

            val jobConfig = Map(
                BPSqlTableJob.URLS_CONFIG_KEY -> urls,
                BPSqlTableJob.TASK_TYPE_CONFIG_KEY -> taskType,
                BPSqlTableJob.TABLE_NAME_CONFIG_KEY -> taskKey.tableName,
                BPSqlTableJob.ERROR_PATH_CONFIG_KEY -> errPaths,
                BPSqlTableJob.DATA_SETS_CONFIG_KEY -> dataSets,
                strategy.jobIdConfigStrategy.TRACE_ID_CONFIG_KEY -> endTask.traceId,
                strategy.jobIdConfigStrategy.JOB_ID_CONFIG_KEY -> endTask.jobId
            )

            val sqlJob = new BPSqlTableJob(this, BPSComponentConfig(sqlId, s"sqlJob-$sqlId", Nil, jobConfig))
            //todo: 等这儿不是chanel线程中运行时就不需要加入线程池了
            ThreadExecutor().execute(new Runnable {
                override def run(): Unit = {
                    sqlJob.open()
                    sqlJob.exec()
                }
            })
            jobs += sqlId -> sqlJob
        }
    }

    private def addJobConfig(task: BPSTypeEvents[HiveTask], providers: List[String]): Unit = {
        providers.toSet.intersect(tableNameMap.keySet).foreach(key => {
            val tableName = tableNameMap(key)
            val taskKey = TaskKey(task.jobId, tableName)
            jobConfigs += taskKey -> (jobConfigs.getOrElse(taskKey, Nil) :+ task.data)
        })
    }

    private def hiveTaskHandle(msg: BPSTypeEvents[HiveTask]): Unit = {
        val data = msg.data
        data.taskType match {
            case "end" => runJob(msg)
            case _ =>
//                val checkJobId = UUID.randomUUID().toString
//                val jobConfig = Map(
//                    BPStreamOverCheckJob.LENGTH_CONFIG_KEY -> data.length.toString,
//                    BPStreamOverCheckJob.ROW_RECORD_PATH_CONFIG_KEY -> data.rowRecordPath,
//                    BPStreamOverCheckJob.METADATA_PATH_CONFIG_KEY -> data.metaDataPath,
//                    strategy.jobIdConfigStrategy.TRACE_ID_CONFIG_KEY -> msg.traceId,
//                    BPStreamOverCheckJob.PUSH_KEY -> CHECK_EVENT_TYPE,
//                    strategy.jobIdConfigStrategy.JOB_ID_CONFIG_KEY -> checkJobId
//                )
//                val checkJob = new BPStreamOverCheckJob(this, BPSComponentConfig(checkJobId, s"hiveCheck-$checkJobId", Nil, jobConfig))
//                jobs += checkJobId -> checkJob
//                tasks += checkJobId -> data
//                checkJob.open()
//                checkJob.exec()
                val ps = BPSConcertEntry.queryComponentWithId("parse schema").get.asInstanceOf[BPSParseSchema]
                val metadata = ps.parseMetadata(data.metaDataPath)(spark)
                val providers = metadata.getOrElse("providers", List("")).asInstanceOf[List[String]]
                addJobConfig(msg, providers)
        }
    }
    case class TaskKey(jobId: String, tableName: String)
}

case class HiveTask(datasetId: String, taskType: String, url: String, length: Long, remarks: String) {
    private val ROW_RECORD_FILE = "row_record"
    private val METADATA_FILE = "metadata"
    private val ERR_FILE = "err"
    private val CONTENT_FILE = "contents"

    val path: String = if (taskType != "end") {
        if (url.contains(CONTENT_FILE)) {
            url.substring(0, url.lastIndexOf(CONTENT_FILE))
        } else {
            "/"
        }
    } else "/"
    val rowRecordPath: String = path + ROW_RECORD_FILE
    val metaDataPath: String = path + METADATA_FILE
    val errPath: String = path + ERR_FILE

    override def toString: String = s"$datasetId, $taskType, $url, $length, $remarks"

}