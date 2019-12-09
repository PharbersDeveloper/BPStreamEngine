package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.{Collections, UUID}

import org.mongodb.scala.bson.ObjectId
import com.pharbers.kafka.schema.{BPJob, DataSet}
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.GithubHelper.BPSGithubHelper
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.json4s.DefaultFormats

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
 *      noticeTopic = “PyJobContainerNoticeTopic” // Job 任务完成通知的`默认` topic, 如果单个 Job 有 Topic， 以 Job 的为准
 *      process = “4” // 默认4，表示 Python Job 的最大并发数
 *      partition = "4" // 默认4，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
 *      retryCount = "3" // 默认, 重试次数
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
    val noticeTopic: String = config.getOrElse("noticeTopic", "PyJobContainerNoticeTopic")
    val process: String = config.getOrElse("process", "4")
    val partition: String = config.getOrElse("partition", "4")
    val retryCount: String = config.getOrElse("retryCount", "3")

    val pythonUri: String = config("pythonUri")
    val pythonBranch: String = config.getOrElse("pythonBranch", "master")

    // 将 python 清洗文件发送到 spark
    val helper = BPSGithubHelper()
    helper.cloneByBranch(id, pythonUri, pythonBranch)
    val pyFiles: List[String] = helper.listFile(id, ".py")
    pyFiles.foreach(spark.sparkContext.addFile)

    // open kafka consumer
    override def open(): Unit = {
        val pkc = new PharbersKafkaConsumer[String, BPJob](
            topics = List(listenerTopic), process = consumerFunc
        )
        ThreadExecutor().execute(pkc)
    }

    import org.json4s.jackson.Serialization.read

    implicit val formats: DefaultFormats.type = DefaultFormats
    val consumerFunc: ConsumerRecord[String, BPJob] => Unit = (record: ConsumerRecord[String, BPJob]) => {
        val jobMsg = read[Map[String, Any]](record.value().getJob.toString)

        // 获得 PyJob 参数信息
        val jobId: String = config("jobId")
        val metadataPath: String = config("metadataPath")
        val filesPath: String = config("filesPath")
        val resultPath: String = config.getOrElse("resultPath", "./jobs/")

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

        inputStream = Some(reading)

        // 真正执行 Job
        val job = BPSPythonJob(jobId, spark, inputStream, this, ???, Map(
            "noticeTopic" -> noticeTopic,
            "resultPath" -> resultPath,
            "lastMetadata" -> metadata,
            "partition" -> partition,
            "retryCount" -> retryCount
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


//    *      parentsOId = "List(parent1, parent2)" // 默认 Nil
//    *      mongoOId = "oid" // 默认 new ObjectId().toString

//    val parentsOId: List[CharSequence] =
//        config.getOrElse("parentsOId", "").split(",").toList.map(_.asInstanceOf[CharSequence])
//    val mongoOId: String = config.getOrElse("mongoOId", new ObjectId().toString)
//
//    var metadata: Map[String, Any] = Map.empty
//    val dir = id
//    val uri = "https://github.com/PharbersDeveloper/bp-data-clean.git"
//    val branch = "v0.0.1"
//

//
//        // 注册血统
//        import collection.JavaConverters._
//        val dfs = new DataSet(
//            parentsOId.asJava,
//            mongoOId,
//            id,
//            Collections.emptyList(),
//            "",
//            metadata("length").asInstanceOf[Double].toInt,
//            s"resultPath/$id/contents",
//            "Python 清洗 Job")
//        BPSBloodJob("data_set_job", dfs).exec()

    override def close(): Unit = {
        super.close()
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
