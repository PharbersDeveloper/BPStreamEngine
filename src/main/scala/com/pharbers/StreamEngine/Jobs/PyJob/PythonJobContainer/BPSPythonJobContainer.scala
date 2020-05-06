package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import java.util.concurrent.TimeUnit
import org.mongodb.scala.bson.ObjectId
import com.pharbers.kafka.schema.HiveTask
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobRemoteListener
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.StreamEngine.Utils.Job.BPSJobContainer
import com.pharbers.StreamEngine.Utils.Strategy.GithubHelper.BPSGithubHelper
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession

object BPSPythonJobContainer {
    def apply(componentProperty: Component2.BPComponentConfig): BPSPythonJobContainer =
        new BPSPythonJobContainer(componentProperty)
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
 *      listens = “Python-FileMetaData” // JobContainer 监听的 topic
 *      startingOffset = “earliest” // kafka 信息处理位置
 *      FileMetaData.msgType = “none” // Job 任务完成通知的`默认` topic, 如果单个 Job 有 Topic， 以 Job 的为准
 *
 *      defaultPartition = "4" // 默认4，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
 *      defaultRetryCount = "3" // 默认, 重试次数
 *
 *      pythonUri = "" // python 清洗代码的 github 路径
 *      pythonBranch = "" // python 清洗代码的分支
 * }}}
 */
@Component(name = "BPSPythonJobContainer", `type` = "BPSPythonJobContainer")
class BPSPythonJobContainer(override val componentProperty: Component2.BPComponentConfig) extends BPSJobContainer {

    override val description: String = "我是一个调用 python 脚本来实现一些清洗任务的节点"

    final private val STARTING_OFFSETS_KEY = "starting.offsets"
    final private val STARTING_OFFSETS_DOC = "kafka offsets begin"
    final private val STARTING_OFFSETS_DEFAULT = "earliest"
    final private val FILE_MSG_TYPE_KEY = "FileMetaData.msgType"
    final private val FILE_MSG_TYPE_DOC = "next job msg type in current job completed"
    final private val FILE_MSG_TYPE_DEFAULT = "none"
    final private val DEFAULT_PARTITION_KEY = "defaultPartition"
    final private val DEFAULT_PARTITION_DOC = "spark default partition"
    final private val DEFAULT_PARTITION_DEFAULT = "4"
    final private val DEFAULT_RETRY_COUNT_KEY = "defaultRetryCount"
    final private val DEFAULT_RETRY_COUNT_DOC = "default retry count"
    final private val DEFAULT_RETRY_COUNT_DEFAULT = "3"
    final private val PYTHON_URI_KEY = "pythonUri"
    final private val PYTHON_URI_DOC = "python code repo uri"
    final private val PYTHON_BRANCH_KEY = "pythonBranch"
    final private val PYTHON_BRANCH_DOC = "python code repo branch"
    final private val PYTHON_BRANCH_DEFAULT = "master"

    type T = BPSCommonJobStrategy
    val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty.config, configDef)

    override val id: String = componentProperty.id
    val containerId: String = id

    override val spark: SparkSession = strategy.getSpark

    override def createConfigDef(): ConfigDef = {
        new ConfigDef()
                .define(STARTING_OFFSETS_KEY, Type.STRING, STARTING_OFFSETS_DEFAULT, Importance.HIGH, STARTING_OFFSETS_DOC)
                .define(FILE_MSG_TYPE_KEY, Type.STRING, FILE_MSG_TYPE_DEFAULT, Importance.HIGH, FILE_MSG_TYPE_DOC)
                .define(DEFAULT_PARTITION_KEY, Type.STRING, DEFAULT_PARTITION_DEFAULT, Importance.HIGH, DEFAULT_PARTITION_DOC)
                .define(DEFAULT_RETRY_COUNT_KEY, Type.STRING, DEFAULT_RETRY_COUNT_DEFAULT, Importance.HIGH, DEFAULT_RETRY_COUNT_DOC)
                .define(PYTHON_URI_KEY, Type.STRING, Importance.HIGH, PYTHON_URI_DOC)
                .define(PYTHON_BRANCH_KEY, Type.STRING, PYTHON_BRANCH_DEFAULT, Importance.HIGH, PYTHON_BRANCH_DOC)
    }

    val startingOffsets: String = strategy.jobConfig.getString(STARTING_OFFSETS_KEY)
    val fileMsgType: String = strategy.jobConfig.getString(FILE_MSG_TYPE_KEY)

    val defaultPartition: String = strategy.jobConfig.getString(DEFAULT_PARTITION_KEY)
    val defaultRetryCount: String = strategy.jobConfig.getString(DEFAULT_RETRY_COUNT_KEY)

    val pythonUri: String = strategy.jobConfig.getString(PYTHON_URI_KEY)
    val pythonBranch: String = strategy.jobConfig.getString(PYTHON_BRANCH_KEY)

    // 将 python 清洗文件发送到 spark
    def sendPy2Spark(): Unit = {
        val helper: BPSGithubHelper =
            BPSConcertEntry.queryComponentWithId("gitRepo").get.asInstanceOf[BPSGithubHelper]
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
    def starJob(event: BPSTypeEvents[Map[String, String]]): Unit = {
        val jobMsg = event.date

        // 获得 PyJob 参数信息
        val jobId: String = jobMsg.getOrElse("jobId", UUID.randomUUID()).toString
        val parentsId: List[CharSequence] = jobMsg.getOrElse("mongoId", "").toString.split(",").toList.map(_.asInstanceOf[CharSequence])
        val datasetId: String = jobMsg.getOrElse("datasetId", new ObjectId()).toString

        val noticeTopic: String = jobMsg.getOrElse("noticeTopic", fileMsgType).toString
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
                .option("startingOffsets", startingOffsets)
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
        logger.info(s"create py job $jobId with config ${jobMsg.mkString(",")}")
        job.open()
        job.exec()
    }

    val noticeFunc: (String, Map[String, Any]) => Unit = { (noticeTopic: String, noticeMap: Map[String, Any]) =>
        val jobId = noticeMap("jobId").toString
        val length = noticeMap("length").asInstanceOf[Long]
        val datasetId = noticeMap("datasetId").toString
        val successPath = noticeMap("successPath").toString

        // TODO 向下通知函数暂时关闭
//        val pkp = new PharbersKafkaProducer[String, HiveTask]
//        val msg = new HiveTask(jobId, "", datasetId, "append", successPath, length.toInt, "")
//        val end = new HiveTask(jobId, "", "", "end", "", 0, "")
//        List(msg, end).foreach(x => {
//            val fu = pkp.produce(noticeTopic, "", x)
//            logger.info(fu.get(10, TimeUnit.SECONDS))
//        })
//        pkp.producer.close()
    }

    override def open(): Unit = {
        logger.info(s"BPSPythonJobContainer containerID is `$containerId`")
        logger.info(s"listener file msg type `${strategy.getListens}`")
    }

    override def exec(): Unit = {
        sendPy2Spark()
        val listenEvent: Seq[String] = strategy.getListens
        val listener: BPJobRemoteListener[Map[String, String]] =
            BPJobRemoteListener[Map[String, String]](this, listenEvent.toList)(x => starJob(x))
        listener.active(null)
        listeners = listener +: listeners
    }

    override def close(): Unit = {
        logger.info(s"close listener file msg type `${strategy.getListens}`")
        BPSConcertEntry
                .queryComponentWithId("gitRepo")
                .get.asInstanceOf[BPSGithubHelper]
                .delDir(id)
        super.close()
    }
}
