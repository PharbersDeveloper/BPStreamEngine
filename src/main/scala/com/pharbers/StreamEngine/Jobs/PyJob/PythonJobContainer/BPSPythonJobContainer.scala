package com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
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
        configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
        val fileSystem: FileSystem = FileSystem.get(configuration)
        val filePath: Path = new Path(path)
        if (!fileSystem.exists(filePath)) {
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
            pyFiles.foreach(spark.sparkContext.addFile)
            val job = BPSPythonJob(id, spark, inputStream, this, Map(
                "resultPath" -> resultPath,
                "metadata" -> metadata
            ))
            job.open()
            job.exec()
        case None => ???
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
