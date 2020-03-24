package com.pharbers.StreamEngine.Jobs.EsJob

import com.pharbers.StreamEngine.Jobs.EsJob.Listener.EsSinkJobCloseListener
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
import com.pharbers.StreamEngine.Utils.Strategy.Schema.BPSParseSchema
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import com.pharbers.StreamEngine.Utils.Job.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import org.apache.kafka.common.config.ConfigDef

object BPSEsSinkJob {
    def apply(id: String,
              spark: SparkSession,
              container: BPSJobContainer,
              jobConf: Map[String, Any]): BPSEsSinkJob =
        new BPSEsSinkJob(id, spark, container, jobConf)
}

/** 执行 EsSink 的 Job
  *
  * @author jeorch
  * @version 0.1
  * @since 2019/11/19 15:43
  * @node 可用的配置参数
  */
class BPSEsSinkJob(override val id: String,
                   override val spark: SparkSession,
                   container: BPSJobContainer,
                   jobConf: Map[String, Any])
        extends BPStreamJob {

    type T = BPStrategyComponent
    override val strategy: BPStrategyComponent = null

    var metadata: Map[String, Any] = Map.empty
    val metadataPath: String = jobConf("metadataPath").toString
    val filesPath: String = jobConf("filesPath").toString
    val indexName: String = jobConf("indexName").toString
    val checkpointLocation: String = jobConf("checkpointLocation").toString
    lazy val hdfsfile: BPSHDFSFile =
        BPSConcertEntry.queryComponentWithId("hdfs").asInstanceOf[BPSHDFSFile]

    // 当所需文件未准备完毕，则等待
    def notFoundShouldWait(path: String): Unit = {
//        if (!BPSHDFSFile.checkPath(path)) {
        if (!hdfsfile.checkPath(path)) {
            logger.debug(path + "文件不存在，等待 1s")
            Thread.sleep(1000)
            notFoundShouldWait(path)
        }
    }

    override def open(): Unit = {
        logger.info("es sink job start with id ========>" + id)
        container.jobs += id -> this
        notFoundShouldWait(metadataPath)
        notFoundShouldWait(filesPath )
        val ps = BPSConcertEntry.queryComponentWithId("parse schema").asInstanceOf[BPSParseSchema]
        metadata = ps.parseMetadata(metadataPath)(spark)
        val loadSchema = ps.parseSchema(metadata("schema").asInstanceOf[List[_]])

        val reading = spark.readStream
            .schema(loadSchema)
            .option("startingOffsets", "earliest")
            .parquet(filesPath)

        inputStream = Some(reading)
    }

    override def exec(): Unit = {

        inputStream match {
            case Some(is) =>
                val query = is.writeStream
                    .option("checkpointLocation", checkpointLocation)
                    .format("es")
                    .start(indexName)
                outputStream = query :: outputStream

                val length = metadata("length").asInstanceOf[Double].toLong
                val listener = EsSinkJobCloseListener(id, id, spark, this, query, length)
                listener.active(null)
                listeners = listener :: listeners

            case None => ???
        }
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
        logger.info("es sink job closed with id ========>" + id)
    }

    override val componentProperty: Component2.BPComponentConfig = null
    override def createConfigDef(): ConfigDef = ???
}
