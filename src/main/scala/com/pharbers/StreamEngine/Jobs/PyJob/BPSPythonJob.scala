package com.pharbers.StreamEngine.Jobs.PyJob

import org.apache.spark.sql
import org.json4s.DefaultFormats
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization.write
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.apache.spark.sql.{Row, ForeachWriter, SparkSession}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

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
                                val hdfsWritePath: Path = new Path("/test/qi/" + id)

                                val configuration: Configuration = new Configuration()
                                configuration.set("fs.defaultFS", "hdfs://spark.master:9000")
//                                configuration.setBoolean("dfs.support.append", true)
//                                configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
//                                configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")

                                val fileSystem: FileSystem = FileSystem.get(configuration)
                                val fsDataOutputStream: FSDataOutputStream =
                                    if (fileSystem.exists(hdfsWritePath))
                                        fileSystem.append(hdfsWritePath)
                                    else
                                        fileSystem.create(hdfsWritePath)

                                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8))

                                true
                            }

                            override def process(value: Row): Unit = {
//                                {"\"Market\"":"\"策略市场\"","\"Province\"":"\"福建省\"",
//                                    "\"City\"":"\"泉州市\"","\"Year\"":"2017","\"Quarter\"":"1",
//                                    "\"Month\"":"1","\"Code\"":"3505004","\"Hospital\"":"NA",
//                                    "\"Level\"":"NA","\"ATC\"":"NA","\"Molecule\"":"NA",
//                                    "\"Product\"":"NA","\"Specificat\"":"NA","\"Size\"":"NA",
//                                    "\"Pack\"":"NA","\"Form\"":"NA","\"Adminst\"":"NA",
//                                    "\"Quantity\"":"3310","\"Value\"":"3846","\"Corporatio\"":"NA",
//                                    "\"TA I\"":"\"WH\"","\"TA II\"":"\"WH\""}
                                val oldStr = value.getAs[String]("data")
                                val newStr = oldStr.replace("\\\"", "")

                                val argv = Array[String]("/usr/bin/python", "./main.py", newStr)
                                val pr = Runtime.getRuntime.exec(argv)
                                val in = new BufferedReader(new InputStreamReader(pr.getInputStream))



                                bufferedWriter.write(in.readLine())
                                bufferedWriter.write("\n")
                            }

                            override def close(errorOrNull: Throwable): Unit = {
                                bufferedWriter.flush()
                                bufferedWriter.close()
                            }

//                            override def process(value: Row): Unit = {
//                                val argv = Array[String]("/usr/bin/python", "./clean.py") //, )
//                                val pr = Runtime.getRuntime.exec(argv)
//                                val in = new BufferedReader(new InputStreamReader(pr.getInputStream))
//
//                                implicit val formats: DefaultFormats.type = DefaultFormats
//
//                                var line: String = in.readLine()
//                                while (line != null) {
//                                    val event = BPSEvents(
//                                        value.getAs[String]("jobId"),
//                                        value.getAs[String]("traceId"),
//                                        value.getAs[String]("type"),
//                                        line,
//                                        value.getAs[java.sql.Timestamp]("timestamp")
//                                    )
//                                    bufferedWriter.write(write(event))
//                                    bufferedWriter.newLine()
//                                    line = in.readLine()
//                                }
//
//                                in.close()
//                                pr.waitFor()
//                            }

//
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
