package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

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
        inputStream match {
            case Some(is) =>
                is.writeStream
                        .foreach(new ForeachWriter[Row]() {
                            var bufferedWriter: BufferedWriter = _

                            override def open(partitionId: Long, version: Long): Boolean = {
                                val configuration: Configuration = new Configuration()
                                configuration.set("fs.defaultFS", "hdfs://192.168.100.137:9000")
                                val fileSystem: FileSystem = FileSystem.get(configuration)

                                //Create a path
                                val hdfsWritePath: Path = new Path("/test/qi/" + id)
                                val fsDataOutputStream: FSDataOutputStream =
                                    if (fileSystem.exists(hdfsWritePath))
                                        fileSystem.append(hdfsWritePath)
                                    else
                                        fileSystem.create(hdfsWritePath)

                                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))

                                true
                            }

                            override def process(value: Row): Unit = {
                                val argv = Array[String]("/usr/bin/python", "./hello_world.py", value.getAs[String]("data"))
                                val pr = Runtime.getRuntime.exec(argv)
                                val in = new BufferedReader(new InputStreamReader(pr.getInputStream))

                                implicit val formats: DefaultFormats.type = DefaultFormats

                                var line: String = in.readLine()
                                while (line != null) {

                                    val event = BPSEvents(
                                        value.getAs[String]("jobId"),
                                        value.getAs[String]("traceId"),
                                        value.getAs[String]("type"),
                                        line,
                                        value.getAs[java.sql.Timestamp]("timestamp")
                                    )
                                    bufferedWriter.write(write(event))
                                    bufferedWriter.newLine()
                                    line = in.readLine()
                                }

                                in.close()
                                pr.waitFor()
                            }

                            override def close(errorOrNull: Throwable): Unit = {
                                bufferedWriter.close()
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
