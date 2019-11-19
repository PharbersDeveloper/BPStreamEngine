package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.json4s.DefaultFormats
import java.nio.charset.StandardCharsets
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StringType
import org.json4s.jackson.Serialization.write
import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jServer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Jobs.PyJob.Listener.BPSProgressListenerAndClose

object BPSPythonJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer,
              jobConf: Map[String, Any]): BPSPythonJob =
        new BPSPythonJob(id, spark, inputStream, container, jobConf)
}

// TODO 目前很多功能还没有定制化，如选择执行的 Python 入口
/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.1
 * @since 2019/11/6 17:43
 * @node 可用的配置参数
 * {{{
 *     fileSuffix = "csv" // 默认
 *     hdfsAddr = "hdfs://spark.master:9000" // 默认
 *     resultPath = "hdfs:///test/sub/"
 *     lastMetadata = Map("jobId" -> "a", "fileName" -> "b")
 * }}}
 */
class BPSPythonJob(override val id: String,
                   override val spark: SparkSession,
                   is: Option[sql.DataFrame],
                   container: BPSJobContainer,
                   jobConf: Map[String, Any]) extends BPStreamJob with Serializable {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    val fileSuffix: String = jobConf.getOrElse("fileSuffix", "csv").toString
    val hdfsAddr: String = jobConf.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString
    val resultPath: String = {
        if (jobConf("resultPath").toString.endsWith("/"))
            jobConf("resultPath").toString + id
        else
            jobConf("resultPath").toString + "/" + id
    }
    val lastMetadata: Map[String, Any] = jobConf("lastMetadata").asInstanceOf[Map[String, Any]]

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        val successPath = resultPath + "/file"
        val errPath = resultPath + "/err"
        val metadataPath = resultPath + "/metadata"
        val checkpointPath = resultPath + "/checkpoint"

        val rowLength = lastMetadata("length").asInstanceOf[String].tail.init.toLong

        inputStream match {
            case Some(is) =>
                val query = is.repartition(1).writeStream
                        .option("checkpointLocation", checkpointPath)
                        .foreach(new ForeachWriter[Row]() {
                            override def open(partitionId: Long, version: Long): Boolean = {
                                val genPath: String => String = path => s"$path/part-$partitionId-$id.$fileSuffix"

                                BPSPy4jServer(rowLength).openBuffer(hdfsAddr)(
                                    genPath(metadataPath),
                                    genPath(successPath),
                                    genPath(errPath)
                                ).startServer().startEndpoint()

                                true
                            }

                            override def process(value: Row): Unit = {
                                val data = value.schema.map { schema =>
                                    schema.dataType match {
                                        case StringType =>
                                            schema.name -> value.getAs[String](schema.name)
                                        case _ => ???
                                    }
                                }.toMap

                                BPSPy4jServer.server.push(
                                    write(Map("metadata" -> lastMetadata, "data" -> data))(DefaultFormats)
                                )
                            }

                            override def close(errorOrNull: Throwable): Unit = {}
                        })
                        .start()
                outputStream = query :: outputStream

                val listener = BPSProgressListenerAndClose(this, query, null)
                listener.active(null)
                listeners = listener :: listeners
            case None => ???
        }
    }

    override def close(): Unit = {
//        logger.info("end =========>>> alfred test")
        super.close()
        container.finishJobWithId(id)
    }
}
