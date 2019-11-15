package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.json4s.DefaultFormats

import scala.util.parsing.json.JSON
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.util.UUID

import com.pharbers.StreamEngine.Jobs.PyJob.Listener.BPSProgressListenerAndClose
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jServer
import org.apache.spark.sql.types.StringType
import py4j.GatewayServer

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
 *     hdfsAddr = "hdfs://spark.master:9000"
 *     resultPath = "hdfs:///test/sub/"
 *     metadata = Map("jobId" -> "a", "fileName" -> "b")
 * }}}
 */
class BPSPythonJob(override val id: String,
                   override val spark: SparkSession,
                   is: Option[sql.DataFrame],
                   container: BPSJobContainer,
                   jobConf: Map[String, Any])
        extends BPStreamJob with Serializable {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    val hdfsAddr: String = jobConf.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString
    val resultPath: String =
        if (jobConf("resultPath").toString.endsWith("/"))
            jobConf("resultPath").toString + id
        else
            jobConf("resultPath").toString + "/" + id
    val metadata: Map[String, Any] = jobConf("metadata").asInstanceOf[Map[String, Any]]

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        val successPath = resultPath + "/file"
        val errPath = resultPath + "/err"
        val metadataPath = resultPath + "/metadata"

        var isFirst = true
        var csvTitle: List[String] = Nil
        inputStream match {
            case Some(is) =>
                val query = is.writeStream
                        .foreach(new ForeachWriter[Row]() {
                            var successBufferedWriter: Option[BufferedWriter] = None
                            var errBufferedWriter: Option[BufferedWriter] = None
                            var metadataBufferedWriter: Option[BufferedWriter] = None
                            var gate: Option[BPSPy4jServer] = None

                            def openHdfs(path: String, partitionId: Long, version: Long): Option[BufferedWriter] = {
                                val configuration: Configuration = new Configuration()
                                configuration.set("fs.defaultFS", hdfsAddr)

                                val fileSystem: FileSystem = FileSystem.get(configuration)
                                val hdfsWritePath = new Path(path + "/" + UUID.randomUUID().toString)

                                val fsDataOutputStream: FSDataOutputStream =
                                    if (fileSystem.exists(hdfsWritePath))
                                        fileSystem.append(hdfsWritePath)
                                    else
                                        fileSystem.create(hdfsWritePath)

                                Some(new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8)))
                            }

                            override def open(partitionId: Long, version: Long): Boolean = {
                                successBufferedWriter =
                                        if (successBufferedWriter.isEmpty) openHdfs(successPath, partitionId, version)
                                        else successBufferedWriter
                                errBufferedWriter =
                                        if (errBufferedWriter.isEmpty) openHdfs(errPath, partitionId, version)
                                        else errBufferedWriter
                                metadataBufferedWriter =
                                        if (metadataBufferedWriter.isEmpty) openHdfs(metadataPath, partitionId, version)
                                        else metadataBufferedWriter
                                gate =
                                    if (gate.isEmpty) {
                                        val tmp = BPSPy4jServer(isFirst, csvTitle)(successBufferedWriter, errBufferedWriter, metadataBufferedWriter)
                                        tmp.startServer()
                                        Some(tmp)
                                    } else gate
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
                                val argv = write(Map("metadata" -> metadata, "data" -> data))(DefaultFormats)
                                if (isFirst) {
//                                    BPSPy4jServer(isFirst, csvTitle)(successBufferedWriter, errBufferedWriter, metadataBufferedWriter).startServer()
                                    Runtime.getRuntime.exec(Array[String]("/usr/bin/python", "./main.py", argv))
                                    isFirst = false
                                }
                            }

                            override def close(errorOrNull: Throwable): Unit = {
                                successBufferedWriter.get.flush()
                                successBufferedWriter.get.close()
                                errBufferedWriter.get.flush()
                                errBufferedWriter.get.close()
                                metadataBufferedWriter.get.flush()
                                metadataBufferedWriter.get.close()
                                gate.get.closeServer()
                            }
                        })
                        .option("checkpointLocation", s"/test/qi2/$id/checkpoint")
                        .start()
                outputStream = query :: outputStream

                val rowLength = metadata("length").asInstanceOf[String].tail.init.toLong

                val listener = BPSProgressListenerAndClose(this, query, rowLength)
                listener.active(null)
                listeners = listener :: listeners
            case None => ???
        }
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
