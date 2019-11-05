package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.json4s.DefaultFormats
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.functions.regexp_replace
import scala.util.parsing.json.JSON

object BPSPythonJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer): BPSPythonJob =
        new BPSPythonJob(id, spark, inputStream, container)
}

class BPSPythonJob(val id: String,
                   val spark: SparkSession,
                   val is: Option[sql.DataFrame],
                   val container: BPSJobContainer) extends BPStreamJob with Serializable {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        val resultPath = "/test/qi/" + id
        val successPath = resultPath + "/file"
        val errPath = resultPath + "/err"
        val metadataPath = resultPath + "/metadata"

        val metaData = spark.sparkContext
                .textFile(s"/test/alex/ff89f6cf-7f52-4ae1-a5ec-2609169b3994/metadata/bf28d-1e0e-4abe-822c-bd09b0")
                .collect()
                .map(_.toString())
                .map { row =>
                    if (row.startsWith("{")) {
                        val tmp = row.split(":")
                        tmp(0).tail.replace("\"", "") -> tmp(1).init.replace("\"", "")
                    } else {
                        "schema" -> JSON.parseFull(row).get
                    }
                }.toMap

        var isFirst = true
        inputStream match {
            case Some(is) =>
                is.writeStream
                        .foreach(new ForeachWriter[Row]() {
                            var successBufferedWriter: BufferedWriter = _
                            var errBufferedWriter: BufferedWriter = _
                            var metadataBufferedWriter: BufferedWriter = _

                            def openHdfs(paht: String): BufferedWriter = {
                                val hdfsWritePath: Path = new Path(paht)

                                val configuration: Configuration = new Configuration()
                                configuration.set("fs.defaultFS", "hdfs://spark.master:9000")

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
                                val argv = write(Map("metadata" -> metaData, "data" -> data))(DefaultFormats)
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
//                        .trigger(Trigger.Continuous("1 second"))
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
