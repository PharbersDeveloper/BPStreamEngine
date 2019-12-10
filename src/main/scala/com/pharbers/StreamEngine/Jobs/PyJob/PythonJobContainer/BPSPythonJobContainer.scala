package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import java.util.concurrent.TimeUnit
import com.pharbers.kafka.schema.BPJob
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.GithubHelper.BPSGithubHelper
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.bson.types.ObjectId
import org.json4s.jackson.Serialization.write

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkJobStrategy,
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
 *      listenerTopic = “PyJobContainerListenerTopic” // JobContainer 监听的 topic
 *      defaultNoticeTopic = “PyJobContainerDefaultNoticeTopic” // Job 任务完成通知的`默认` topic, 如果单个 Job 有 Topic， 以 Job 的为准
 *      process = “4” // 默认4，表示 Python Job 的最大并发数
 *      defaultPartition = "4" // 默认4，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
 *      defaultRetryCount = "3" // 默认, 重试次数
 *      pythonUri = "" // python 清洗代码的 github 路径
 *      pythonBranch = "" // python 清洗代码的分支
 * }}}
 */
class BPSPythonJobContainer(override val spark: SparkSession, config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob with Serializable {

    type T = BPSKfkJobStrategy
    override val strategy: BPSKfkJobStrategy = null

    override val id: String = config.getOrElse("containerId", UUID.randomUUID().toString)
    val listenerTopic: String = config.getOrElse("listenerTopic", "PyJobContainerListenerTopic")
    val defaultNoticeTopic: String = config.getOrElse("defaultNoticeTopic", "PyJobContainerDefaultNoticeTopic")
    val process: String = config.getOrElse("process", "4")
    val defaultPartition: String = config.getOrElse("defaultPartition", "4")
    val defaultRetryCount: String = config.getOrElse("defaultRetryCount", "3")

    val pythonUri: String = config("pythonUri")
    val pythonBranch: String = config.getOrElse("pythonBranch", "master")

    // 将 python 清洗文件发送到 spark
    val helper = BPSGithubHelper()
    helper.cloneByBranch(id, pythonUri, pythonBranch)
    val pyFiles: List[String] = helper.listFile(id, ".py")
    pyFiles.foreach(spark.sparkContext.addFile)

    // open kafka consumer
    override def open(): Unit = {
        logger.info(s"open kafka consumer, listener topic is `$listenerTopic`")
        val pkc = new PharbersKafkaConsumer(topics = List(listenerTopic), process = consumerFunc)
        ThreadExecutor().execute(pkc)
    }

    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    // TODO 需要并发管理
    val consumerFunc: ConsumerRecord[String, BPJob] => Unit = { (record: ConsumerRecord[String, BPJob]) =>
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
        val jobMsg = parse(record.value().getJob.toString).extract[Map[String, Any]]

        // 获得 PyJob 参数信息
        val jobId: String = jobMsg("jobId").toString
        val metadataPath: String = jobMsg("metadataPath").toString
        val filesPath: String = jobMsg("filesPath").toString
        val resultPath: String = jobMsg.getOrElse("resultPath", "./jobs/").toString
        val noticeTopic: String = jobMsg.getOrElse("noticeTopic", defaultNoticeTopic).toString
        val partition: String = jobMsg.getOrElse("partition", defaultPartition).toString
        val retryCount: String = jobMsg.getOrElse("retryCount", defaultRetryCount).toString
        val parentsOId: String = jobMsg.getOrElse("parentsOId", "").toString
        val mongoOId: String = jobMsg.getOrElse("mongoOId", new ObjectId().toString).toString

        // 检查输入数据是否存在
        notFoundShouldWait(metadataPath)
        notFoundShouldWait(filesPath)

        // 解析 metadata
        val metadata = BPSParseSchema.parseMetadata(metadataPath)(spark)
        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])

        // 读取输入流
        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet(filesPath)

        // 真正执行 Job
        val job = BPSPythonJob(jobId, spark, Some(reading), this, noticeFunc, Map(
            "noticeTopic" -> noticeTopic,
            "resultPath" -> resultPath,
            "lastMetadata" -> metadata,
            "partition" -> partition,
            "retryCount" -> retryCount,
            "parentsOId" -> parentsOId,
            "mongoOId" -> mongoOId
        ))
        job.open()
        job.exec()
    }

    // 当所需文件未准备完毕，则等待
    def notFoundShouldWait(path: String): Unit = {
        if (!BPSHDFSFile.checkPath(path)) {
            logger.debug(path + "文件不存在，等待 1s")
            Thread.sleep(1000)
            notFoundShouldWait(path)
        }
    }

    val noticeFunc: (String, Map[String, Any]) => Unit = { (topic, msg) =>
        implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

        val pkp = new PharbersKafkaProducer[String, BPJob]
        val bpJob = new BPJob("", "", "", write(msg))
        val fu = pkp.produce(topic, "", bpJob)
        logger.debug(fu.get(10, TimeUnit.SECONDS))
    }

    // TODO 需要关闭 Consumer
    override def close(): Unit = {
        helper.delDir(id)
        super.close()
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
