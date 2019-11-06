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

object BPSPythonJob {
    def apply(id: String, spark: SparkSession,
              inputStream: Option[sql.DataFrame], container: BPSJobContainer,
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
class BPSPythonJob(override val id: String, override val spark: SparkSession,
                   is: Option[sql.DataFrame], container: BPSJobContainer,
                   jobConf: Map[String, Any]) extends BPStreamJob with Serializable {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    val hdfsAddr: String = jobConf.getOrElse("hdfsAddr", "hdfs://spark.master:9000").toString
    val resultPath: String =
        if(jobConf("resultPath").toString.endsWith("/"))
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
        inputStream match {
            case Some(is) =>
                is.writeStream
                        .foreach(new ForeachWriter[Row]() {
                            var successBufferedWriter: BufferedWriter = _
                            var errBufferedWriter: BufferedWriter = _
                            var metadataBufferedWriter: BufferedWriter = _

                            def openHdfs(path: String): BufferedWriter = {
                                val hdfsWritePath: Path = new Path(path)

                                val configuration: Configuration = new Configuration()
                                configuration.set("fs.defaultFS", hdfsAddr)

                                val fileSystem: FileSystem = FileSystem.get(configuration)
                                val fsDataOutputStream: FSDataOutputStream =
                                    if (fileSystem.exists(hdfsWritePath))
                                        fileSystem.append(hdfsWritePath)
                                    else
                                        fileSystem.create(hdfsWritePath)

                                new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))
                            }

                            override def open(partitionId: Long, version: Long): Boolean = {
                                successBufferedWriter = openHdfs(successPath)
                                errBufferedWriter = openHdfs(errPath)
                                metadataBufferedWriter = openHdfs(metadataPath)
                                true
                            }

                            override def process(value: Row): Unit = {
                                val data = JSON.parseFull(value.getAs[String]("data").replace("\\\"", "")).get
                                val argv = write(Map("metadata" -> metadata, "data" -> data))(DefaultFormats)
                                val pyArgs = Array[String]("/usr/bin/python", "./main.py", argv)
                                val pr = Runtime.getRuntime.exec(pyArgs)
                                val in = new BufferedReader(new InputStreamReader(pr.getInputStream))

                                var lines = in.readLine()
                                while (lines != null) {
                                    JSON.parseFull(lines) match {
                                        case Some(result: Map[String, AnyRef]) =>
                                            if (result("tag").asInstanceOf[Double] == 1) {
                                                successBufferedWriter.write(write(result("data"))(DefaultFormats))
                                                successBufferedWriter.write("\n")
                                                if(isFirst){
                                                    metadataBufferedWriter.write(write(result("metadata"))(DefaultFormats))
                                                    isFirst = false
                                                }
                                            } else {
                                                errBufferedWriter.write(lines)
                                                errBufferedWriter.write("\n")
                                            }
                                        case None =>
                                            errBufferedWriter.write(lines)
                                            errBufferedWriter.write("\n")
                                    }
                                    lines = in.readLine()
                                }
                            }

                            override def close(errorOrNull: Throwable): Unit = {
                                successBufferedWriter.flush()
                                successBufferedWriter.close()
                                errBufferedWriter.flush()
                                errBufferedWriter.close()
                                metadataBufferedWriter.flush()
                                metadataBufferedWriter.close()
                            }
                        })
                        .start()
                        .awaitTermination()
            case None => ???
        }
    }

    override def close(): Unit = {
        super.close()
        container.finishJobWithId(id)
    }
}
