package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.mongodb.scala.bson.ObjectId
import org.apache.spark.sql.SparkSession
import com.pharbers.kafka.schema.{BPJob, HiveTask}
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.Strategy.BPSKfkBaseStrategy
import com.pharbers.StreamEngine.Utils.Strategy.GithubHelper.BPSGithubHelper
import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.apache.kafka.common.config.ConfigDef

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkBaseStrategy,
              spark: SparkSession,
              config: Map[String, String]): BPSPythonJobContainer =
        new BPSPythonJobContainer(spark, config)
}

/** 执行 Python 清洗的 Container
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:43
 * @node config 可用的配置参数
 * {{{
 *      containerId = UUID // 缺省 UUID
 *
 *      listenerTopic = “PyJobContainerListenerTopic” // JobContainer 监听的 topic
 *      defaultNoticeTopic = “PyJobContainerNoticeTopic” // Job 任务完成通知的`默认` topic, 如果单个 Job 有 Topic， 以 Job 的为准
 *
 *      defaultPartition = "4" // 默认4，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
 *      defaultRetryCount = "3" // 默认, 重试次数
 *
 *      pythonUri = "" // python 清洗代码的 github 路径
 *      pythonBranch = "" // python 清洗代码的分支
 * }}}
 */
class BPSPythonJobContainer(override val spark: SparkSession,
                            config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob with Serializable {

    type T = BPSKfkBaseStrategy
    override val strategy: BPSKfkBaseStrategy = null

    override val id: String = config.getOrElse("containerId", UUID.randomUUID().toString)
    val containerId: String = id

    val listenerTopic: String = config.getOrElse("listenerTopic", "PyJobContainerListenerTopic")
    val defaultNoticeTopic: String = config.getOrElse("noticeTopic", "PyJobContainerNoticeTopic")

    val defaultPartition: String = config.getOrElse("defaultPartition", "4")
    val defaultRetryCount: String = config.getOrElse("defaultRetryCount", "3")

    val pythonUri: String = config("pythonUri")
    val pythonBranch: String = config.getOrElse("pythonBranch", "master")

    // 将 python 清洗文件发送到 spark
    def sendPy2Spark(): Unit = {
        val helper: BPSGithubHelper =
            BPSConcertEntry.queryComponentWithId("").get.asInstanceOf[BPSGithubHelper]
        helper.cloneByBranch(containerId, pythonUri, pythonBranch)
        val pyFiles: List[String] = helper.listFile(containerId, ".py")
        pyFiles.foreach(spark.sparkContext.addFile)
    }

    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    /** kafka consumer 收到消息的 function
     *
     * @author clock
     * @version 0.0.1
     * @since 2020/3/23 14:08
     * @node config 可用的配置参数
     * {{{
     *      jobId = UUID // 缺省 UUID
     *      parentsId = ObjectId // 缺省 ""
     *      datasetId = ObjectId  // 缺省 new ObjectId().toString
     *
     *      noticeTopic = defaultNoticeTopic // Job 任务完成通知topic, 如果单个 Job 有 Topic， 以 Job 的为准，如果 Job 没用，使用默认 Topic
     *      partition = defaultPartition //默认 “4”，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
     *      retryCount = defaultRetryCount // 默认, 重试次数
     *
     *      metadataPath = "" // 流数据的元信息
     *      filesPath = "" // 流数据存放位置
     *      resultPath = "./jobs/" // 默认"./jobs/"，Job 执行后的结果的存放位置, 会自动添加 containerId
     * }}}
     */
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val consumerFunc: ConsumerRecord[String, BPJob] => Unit = { record: ConsumerRecord[String, BPJob] =>
        val jobMsg = parse(record.value().getJob.toString).extract[Map[String, Any]]

        // 获得 PyJob 参数信息
        val jobId: String = jobMsg.getOrElse("jobId", UUID.randomUUID()).toString
        val parentsId: List[CharSequence] = jobMsg.getOrElse("parentsId", "").toString.split(",").toList.map(_.asInstanceOf[CharSequence])
        val datasetId: String = jobMsg.getOrElse("datasetId", new ObjectId()).toString

        val noticeTopic: String = jobMsg.getOrElse("noticeTopic", defaultNoticeTopic).toString
        val partition: String = jobMsg.getOrElse("partition", defaultPartition).toString
        val retryCount: String = jobMsg.getOrElse("retryCount", defaultRetryCount).toString

        val metadataPath: String = jobMsg("metadataPath").toString
        val filesPath: String = jobMsg("filesPath").toString
        val resultPath: String = {
            val path = jobMsg.getOrElse("resultPath", "./jobs/").toString
            if (path.endsWith("/")) path + containerId
            else path + "/" + containerId
        }

        val ps = BPSConcertEntry.queryComponentWithId("parse schema").get.asInstanceOf[BPSParseSchema]
        val metadata = ps.parseMetadata(metadataPath)(spark)
        val loadSchema = ps.parseSchema(metadata("schema").asInstanceOf[List[_]])

        // 读取输入流
        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                //TODO: 设置触发的文件数，以控制内存 效果待测试
                .option("maxFilesPerTrigger", partition.toInt)
                .parquet(filesPath)

        // 真正执行 Job
        val job = BPSPythonJob(jobId, spark, Some(reading), noticeFunc, finishJobWithId, Map(
            "noticeTopic" -> noticeTopic,
            "datasetId" -> datasetId,
            "parentsId" -> parentsId,
            "resultPath" -> resultPath,
            "lastMetadata" -> metadata,
            "fileSuffix" -> "csv",
            "partition" -> partition,
            "retryCount" -> retryCount
        ))

        job.open()
        job.exec()
    }

    val noticeFunc: (String, Map[String, Any]) => Unit = { (noticeTopic: String, noticeMap: Map[String, Any]) =>
        val jobId = noticeMap("jobId").toString
        val length = noticeMap("length").asInstanceOf[Long]
        val datasetId = noticeMap("datasetId").toString
        val successPath = noticeMap("successPath").toString

        val pkp = new PharbersKafkaProducer[String, HiveTask]
        val msg = new HiveTask(jobId, "", datasetId, "append", successPath, length.toInt, "")
        val end = new HiveTask(jobId, "", "", "end", "", 0, "")
        List(msg, end).foreach(x => {
            val fu = pkp.produce(noticeTopic, "", x)
            logger.info(fu.get(10, TimeUnit.SECONDS))
        })
        pkp.producer.close()
    }

    // open kafka consumer
    var pyConsumer: Option[PharbersKafkaConsumer[String, BPJob]] = None

    override def open(): Unit = {
        logger.info(s"BPSPythonJobContainer containerID is `$containerId`")
        sendPy2Spark()
        if (pyConsumer.isEmpty) {
            logger.info(s"open kafka consumer, listener topic is `$listenerTopic`")
            val pkc = new PharbersKafkaConsumer(topics = List(listenerTopic), process = consumerFunc)
            ThreadExecutor().execute(pkc)
            pyConsumer = Some(pkc)
        } else {
            logger.info(s"kafka consumer is opened")
        }
    }

    override def close(): Unit = {
        logger.info(s"close kafka consumer, listener topic is `$listenerTopic`")
        pyConsumer.get.close()
        val helper: BPSGithubHelper =
            BPSConcertEntry.queryComponentWithId("").get.asInstanceOf[BPSGithubHelper]
        helper.delDir(id)
        super.close()
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???

    override val description: String = "py_clean_job"
}
