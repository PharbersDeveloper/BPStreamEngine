package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.{Collections, UUID}
import org.mongodb.scala.bson.ObjectId
import com.pharbers.kafka.schema.DataSet
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob.BPSBloodJob
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer}

object BPSPythonJobContainer {
    def apply(strategy: BPSKfkJobStrategy,
              spark: SparkSession,
              config: Map[String, String]): BPSPythonJobContainer =
        new BPSPythonJobContainer(spark, config)
}

/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:43
 * @node 可用的配置参数
 * {{{
 *      jobId = UUID // 默认
 *      matedataPath = "/user/clock/jobs/matedataPath/$lastJobID"
 *      filesPath = "/user/clock/jobs/filesPath/$lastJobID"
 *      resultPath = "/user/clock/jobs/resultPath" // 后面会自动加上当前的 jobId
 *      partition = "4" // 默认4
 *
 *      parentsOId = "List(parent1, parent2)" // 默认 Nil
 *      mongoOId = "oid" // 默认 new ObjectId().toString
 * }}}
 */
class BPSPythonJobContainer(override val spark: SparkSession,
                            config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob with Serializable {

    override val strategy: BPSKfkJobStrategy = null
    type T = BPSKfkJobStrategy

    val id: String = config.getOrElse("jobId", UUID.randomUUID().toString).toString
    val matedataPath: String = config("matedataPath").toString
    val filesPath: String = config("filesPath").toString
    val resultPath: String = config.getOrElse("resultPath", "./jobs/").toString
    val partition: String = config.getOrElse("partition", "4").toString

    val parentsOId: List[CharSequence] =
        config.getOrElse("parentsOId", Nil).asInstanceOf[List[String]].map(_.asInstanceOf[CharSequence])
    val mongoOId: String = config.getOrElse("mongoOId", new ObjectId().toString).toString

    var metadata: Map[String, Any] = Map.empty
    val pyFiles = List(
        "./pyClean/main.py",
        "./pyClean/results.py",
        "./pyClean/auth.py",
        "./pyClean/mapping.py",
        "./pyClean/cleaning.py"
    )

    // 当所需文件未准备完毕，则等待
    def notFoundShouldWait(path: String): Unit = {
        if (!BPSHDFSFile.checkPath(path)) {
            logger.debug(path + "文件不存在，等待 1s")
            Thread.sleep(1000)
            notFoundShouldWait(path)
        }
    }

    override def open(): Unit = {
        notFoundShouldWait(matedataPath)
        notFoundShouldWait(filesPath)

        metadata = BPSParseSchema.parseMetadata(matedataPath)(spark)
        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])

        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                .parquet(filesPath)

        inputStream = Some(reading)

        // 注册血统
//        import collection.JavaConverters._
//        val dfs = new DataSet(
//            parentsOId.asJava,
//            mongoOId,
//            id,
//            Collections.emptyList(),
//            "",
//            metadata("length").asInstanceOf[String].toInt,
//            resultPath,
//            "description")
//        BPSBloodJob("data_set_job", dfs).exec()
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            //todo: 为了submit后能使用，时使用--file预先加入了file。之后可以选择将py文件放在hdfs中，这儿根据配置的hdfs目录加载
            pyFiles.foreach(spark.sparkContext.addFile)
            val job = BPSPythonJob(id, spark, inputStream, this, Map(
                "resultPath" -> resultPath,
                "lastMetadata" -> metadata,
                "partition" -> partition
            ))
            job.open()
            job.exec()
        case None => ???
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
