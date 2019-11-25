package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import java.util.UUID
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.pharbers.StreamEngine.Jobs.PyJob.BPSPythonJob
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
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
 * @version 0.1
 * @since 2019/11/6 17:43
 * @node 可用的配置参数
 * {{{
 *      hdfsAddr = "hdfs://spark.master:9000"
 *      resultPath = "hdfs:///test/sub/"
 *      metadata = Map("jobId" -> "a", "fileName" -> "b")
 * }}}
 */
class BPSPythonJobContainer(override val spark: SparkSession,
                            config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob with Serializable {

    override val strategy: BPSKfkJobStrategy = null
    type T = BPSKfkJobStrategy

    var metadata: Map[String, Any] = Map.empty

    val id: String = config("jobId").toString
    val matedataPath: String = config("matedataPath").toString
    val filesPath: String = config("filesPath").toString
    val resultPath: String = config("resultPath").toString
    val hdfsAddr: String = config.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString
    val pyFiles = List(
        "./pyClean/main.py",
        "./pyClean/results.py",
        "./pyClean/auth.py",
        "./pyClean/mapping.py",
        "./pyClean/cleaning.py"
    )

    // 当所需文件未准备完毕，则等待
    def notFoundShouldWait(path: String): Unit = {
        val configuration: Configuration = new Configuration
        configuration.set("fs.defaultFS", hdfsAddr)
        val fileSystem: FileSystem = FileSystem.get(configuration)
        if (!fileSystem.exists(new Path(path))) {
            logger.debug(path + "文件不存在，等待 1s")
            Thread.sleep(1000)
            notFoundShouldWait(path)
        }
    }

    override def open(): Unit = {
        notFoundShouldWait(matedataPath + id)
        //不全是path + jobid， 可能是path + jobid + file
        notFoundShouldWait(filesPath )
        metadata = BPSParseSchema.parseMetadata(matedataPath + id)(spark)
        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])

        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                //不全是path + jobid， 可能是path + jobid + file
                .parquet(filesPath)

        inputStream = Some(reading)
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            //todo: 为了submit后能使用，时使用--file预先加入了file。之后可以选择将py文件放在hdfs中，这儿根据配置的hdfs目录加载
            pyFiles.foreach(spark.sparkContext.addFile)
            val job = BPSPythonJob(UUID.randomUUID().toString, spark, inputStream, this, Map( //UUID.randomUUID().toString
                "resultPath" -> resultPath,
                "lastMetadata" -> metadata
            ))
            job.open()
            job.exec()
        case None => ???
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
