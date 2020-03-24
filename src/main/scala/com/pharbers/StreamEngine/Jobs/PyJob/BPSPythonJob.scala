package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID

import org.apache.spark.sql
import org.json4s.DefaultFormats
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.StringType
import org.json4s.jackson.Serialization.write
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jManager
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Jobs.PyJob.Listener.BPSProgressListenerAndClose
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.kafka.schema.HiveTask
import org.apache.kafka.common.config.ConfigDef

object BPSPythonJob {
    def apply(id: String,
              spark: SparkSession,
              inputStream: Option[sql.DataFrame],
              container: BPSJobContainer,
              jobConf: Map[String, Any]): BPSPythonJob =
        new BPSPythonJob(id, spark, inputStream, container, jobConf)
}

/** 执行 Python 的 Job
 *
 * @author clock
 * @version 0.0.1
 * @since 2019/11/6 17:43
 * @node 可用的配置参数
 * {{{
 *     fileSuffix = "csv" // 默认
 *     resultPath = "/users/clock/jobs/"
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
    val resultPath: String = {
        if (jobConf("resultPath").toString.endsWith("/"))
            jobConf("resultPath").toString + id
        else
            jobConf("resultPath").toString + "/" + id
    }
    val lastMetadata: Map[String, Any] = jobConf("lastMetadata").asInstanceOf[Map[String, Any]]
    val partition: Int = jobConf.getOrElse("partition", "4").asInstanceOf[String].toInt

    val checkpointPath = resultPath + "/checkpoint"
    val rowRecordPath = resultPath + "/row_record"
    val metadataPath = resultPath + "/metadata"
    val successPath = resultPath + "/contents"
    val errPath = resultPath + "/err"

    override def open(): Unit = {
        inputStream = is
    }

    override def exec(): Unit = {
        implicit val py4jManager: BPSPy4jManager = BPSPy4jManager()

        inputStream match {
            case Some(is) =>
                val query = is
                        //todo: 本来是为了通过重新分区来提高并行度，但是会产生shuffle。测试通过使用读取文件数量来分区
//                        .repartition(partition)
                        .writeStream
                        .option("checkpointLocation", checkpointPath)
                        .foreach(new ForeachWriter[Row]() {

                            override def open(partitionId: Long, version: Long): Boolean = {
                                val threadId: String = UUID.randomUUID().toString
                                val genPath: String => String = path => s"$path/part-$partitionId-$threadId.$fileSuffix"

                                py4jManager.open(Map(
                                    "py4jManager" -> py4jManager,
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
                                while (py4jManager.dataQueue.nonEmpty) {
                                    Thread.sleep(1000)
                                }
                            }
                        })
                        .start()

                outputStream = query :: outputStream

                val rowLength = lastMetadata("length").asInstanceOf[Double].toLong
                val listener = BPSProgressListenerAndClose(this, spark, rowLength, rowRecordPath)
                listener.active(null)
                listeners = listener :: listeners
            case None => ???
        }
    }

    override def close(): Unit = {
        super.close()
        //todo： 这儿需要配置
        val topic = "HiveTask"
        val pkp = new PharbersKafkaProducer[String, HiveTask]
        val msg = new HiveTask(id, "", jobConf("mongoId").toString, "append", successPath, lastMetadata("length").asInstanceOf[Int], "")
        val end = new HiveTask(id, "", "", "end", "", 0, "")
        List(msg, end).foreach(x => {
            val fu = pkp.produce(topic, "", x)
            logger.info(fu.get(10, TimeUnit.SECONDS))
        })
        pkp.producer.close()
        container.finishJobWithId(id)
    }

    override val componentProperty: Component2.BPComponentConfig = null

    override def createConfigDef(): ConfigDef = ???
}
