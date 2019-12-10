package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID
import org.apache.spark.sql
import org.json4s.DefaultFormats
import org.apache.spark.sql.types.StringType
import org.json4s.jackson.Serialization.write
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jManager
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Jobs.PyJob.Listener.BPSProgressListenerAndClose

object BPSPythonJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer,
              noticeFunc: (String, Map[String, Any]) => Unit,
              jobConf: Map[String, Any]): BPSPythonJob =
        new BPSPythonJob(id, spark, inputStream, container, noticeFunc, jobConf)
}

/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:43
 * @node jobConf 可用的配置参数
 * {{{
 *     noticeTopic = "topic" //job完成后的通知位置
 *     resultPath = "/users/clock/jobs/"
 *     lastMetadata = Map("jobId" -> "a", "fileName" -> "b")
 *
 *     fileSuffix = "csv" // 默认
 *     partition = "4" //默认 “4”，表示每个 Job 可使用的 spark 分区数，也是可用 Python 的线程数，默认 4 线程
 *     retryCount = "3" // 默认, 重试次数
 * }}}
 */
class BPSPythonJob(override val id: String,
                   override val spark: SparkSession,
                   is: Option[sql.DataFrame],
                   container: BPSJobContainer,
                   noticeFunc: (String, Map[String, Any]) => Unit,
                   jobConf: Map[String, Any]) extends BPStreamJob with Serializable {

    type T = BPSJobStrategy
    override val strategy: BPSJobStrategy = null

    val noticeTopic: String = jobConf("noticeTopic").toString
    val resultPath: String = {
        if (jobConf("resultPath").toString.endsWith("/"))
            jobConf("resultPath").toString + id
        else
            jobConf("resultPath").toString + "/" + id
    }
    val lastMetadata: Map[String, Any] = jobConf("lastMetadata").asInstanceOf[Map[String, Any]]

    val fileSuffix: String = jobConf.getOrElse("fileSuffix", "csv").toString
    val partition: Int = jobConf.getOrElse("partition", "4").asInstanceOf[String].toInt
    val retryCount: String = jobConf.getOrElse("retryCount", "3").toString

    val checkpointPath: String = resultPath + "/checkpoint"
    val rowRecordPath: String = resultPath + "/row_record"
    val metadataPath: String = resultPath + "/metadata"
    val successPath: String = resultPath + "/contents"
    val errPath: String = resultPath + "/err"

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {

        implicit val py4jManager: BPSPy4jManager = BPSPy4jManager()

        inputStream match {
            case Some(is) =>
                val query = is.repartition(partition).writeStream
                        .option("checkpointLocation", checkpointPath)
                        .foreach(new ForeachWriter[Row]() {
                            override def open(partitionId: Long, version: Long): Boolean = {
                                val threadId: String = UUID.randomUUID().toString
                                val genPath: String => String = path => s"$path/part-$partitionId-$threadId.$fileSuffix"

                                py4jManager.open(Map(
                                    "retryCount" -> retryCount,
                                    "jobId" -> id,
                                    "threadId" -> threadId,
                                    "rowRecordPath" -> genPath(rowRecordPath),
                                    "successPath" -> genPath(successPath),
                                    "errPath" -> genPath(errPath),
                                    "metadataPath" -> genPath(metadataPath)
                                ))

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

                                py4jManager.push(
                                    write(Map("metadata" -> lastMetadata, "data" -> data))(DefaultFormats)
                                )
                            }

                            override def close(errorOrNull: Throwable): Unit = {
                                py4jManager.push("EOF")
                            }
                        })
                        .start()

                outputStream = query :: outputStream
                listeners = listeners ::: addListener(rowRecordPath) :: Nil
            case None => ???
        }
    }

    override def close(): Unit = {
        noticeFunc(noticeTopic, Map(
            "id" -> id,
            "resultPath" -> resultPath,
            "rowRecordPath" -> rowRecordPath,
            "metadataPath" -> metadataPath,
            "successPath" -> successPath,
            "errPath" -> errPath
        ))
        super.close()
        container.finishJobWithId(id)
    }

    def addListener(rowRecordPath: String): BPStreamListener = {
        val rowLength = lastMetadata("length").asInstanceOf[Double].toLong
        val listener = BPSProgressListenerAndClose(this, spark, rowLength, rowRecordPath)
        listener.active(null)
        listener
    }
}
