package com.pharbers.StreamEngine.Jobs.EsJob.EsJobContainer

import com.pharbers.StreamEngine.Jobs.EsJob.BPSEsSinkJob
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import com.pharbers.StreamEngine.Utils.Schema.Spark.BPSParseSchema
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer}

object BPSEsSinkJobContainer {
    def apply(strategy: BPSKfkJobStrategy,
              spark: SparkSession,
              config: Map[String, String]): BPSEsSinkJobContainer =
        new BPSEsSinkJobContainer(spark, config)
}

/** 执行 EsSink 的 Job
 *
 * @author jeorch
 * @version 0.1
 * @since 2019/11/19 15:43
 * @node 可用的配置参数
 */
class BPSEsSinkJobContainer(override val spark: SparkSession,
                            config: Map[String, String])
        extends BPSJobContainer with BPDynamicStreamJob {

    override val strategy: BPSKfkJobStrategy = null
    type T = BPSKfkJobStrategy

    var metadata: Map[String, Any] = Map.empty

    val id: String = config("jobId").toString
    val matedataPath: String = config("matedataPath").toString
    val filesPath: String = config("filesPath").toString
    val indexName: String = config("indexName").toString

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
        notFoundShouldWait(matedataPath)
        //不全是path + jobid， 可能是path + jobid + file
        notFoundShouldWait(filesPath )
        metadata = BPSParseSchema.parseMetadata(matedataPath)(spark)
        val loadSchema = BPSParseSchema.parseSchema(metadata("schema").asInstanceOf[List[_]])

        println(loadSchema)

        val reading = spark.readStream
                .schema(loadSchema)
                .option("startingOffsets", "earliest")
                //不全是path + jobid， 可能是path + jobid + file
                .parquet(filesPath)

        inputStream = Some(reading)
    }

    override def exec(): Unit = inputStream match {
        case Some(_) =>
            val job = BPSEsSinkJob(id, spark, inputStream, this, Map(
                "indexName" -> indexName
            ))
            job.open()
            job.exec()
        case None => ???
    }

    override def registerListeners(listener: BPStreamListener): Unit = {}

    override def handlerExec(handler: BPSEventHandler): Unit = {}
}
