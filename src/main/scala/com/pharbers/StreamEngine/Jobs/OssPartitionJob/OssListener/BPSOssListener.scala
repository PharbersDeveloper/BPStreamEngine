package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.HDFS.BPSHDFSFile
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.FileMetaData
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/11 13:30
  * @note 一些值得注意的地方
  */
case class BPSOssListener(spark: SparkSession, job: BPStreamJob, jobId: String) extends BPStreamRemoteListener {
    import spark.implicits._
    def event2JobId(e: BPSEvents): String = e.jobId

    override def trigger(e: BPSEvents): Unit = {
        val runId = job.asInstanceOf[BPSJobContainer].id
        // TODO: 后面可变配置化
        val genPath = s"/jobs/$runId/$jobId"
	    val metaDataPath = s"$genPath/metadata"
        val sampleDataPath = s"$genPath/contents"
        
        e.`type` match {
            case "SandBox-Schema" => {
//                BPSOssPartitionMeta.pushLineToHDFS(runId.id, event2JobId(e), e.data)
                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
            }
            case "SandBox-Labels" => {
                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
            }
            case "SandBox-Length" => {
                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
	            //TODO： 需要改TS的接口,后面改成Kafka
//                post(s"""{"traceId": "${e.traceId}","jobId": "${e.jobId}"}""", "application/json")
	            try {
                    pollKafka(new FileMetaData(runId, e.jobId, metaDataPath, sampleDataPath, ""))
                } catch {
                    case ex: Exception =>
                        pollKafka(new FileMetaData(runId, e.jobId, metaDataPath, sampleDataPath, ""))
                }
                
            }
        }
    }

    override def hit(e: BPSEvents): Boolean = e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Labels" || e.`type` == "SandBox-Length"

    override def active(s: DataFrame): Unit = {
        BPSDriverChannel.registerListener(this)

        job.outputStream = s.filter($"type" === "SandBox-Schema" || $"type" === "SandBox-Labels" || $"type" === "SandBox-Length").writeStream
                .foreach(
                    new ForeachWriter[Row] {

                        var channel: Option[BPSWorkerChannel] = None

                        def open(partitionId: Long, version: Long): Boolean = {
                            if (channel.isEmpty) channel = Some(BPSWorkerChannel(TaskContext.get().getLocalProperty("host")))
                            true
                        }

                        def process(value: Row) : Unit = {

                            implicit val formats = DefaultFormats

                            val event = BPSEvents(
                                value.getAs[String]("jobId"),
                                value.getAs[String]("traceId"),
                                value.getAs[String]("type"),
                                value.getAs[String]("data"),
                                value.getAs[java.sql.Timestamp]("timestamp")
                            )
                            channel.get.pushMessage(write(event))
                        }

                        def close(errorOrNull: scala.Throwable): Unit = {}//channel.get.close()
                    }
                )
                .option("checkpointLocation", "/jobs/" + UUID.randomUUID().toString + "/checkpoint")
                .start() :: job.outputStream
    }

    override def deActive(): Unit = {
        BPSDriverChannel.unRegisterListener(this)
    }

    def pollKafka(msg: FileMetaData): Unit ={
        //todo: 参数化
        val topic = "sb_file_meta_job"
        val pkp = new PharbersKafkaProducer[String, FileMetaData]
        val fu = pkp.produce(topic, msg.getJobId.toString, msg)
        logger.info(fu.get(10, TimeUnit.SECONDS))
    }
}
