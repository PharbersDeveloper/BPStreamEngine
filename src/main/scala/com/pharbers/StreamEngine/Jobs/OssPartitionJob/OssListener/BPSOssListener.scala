package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener

import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionMeta.BPSOssPartitionMeta
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
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
case class BPSOssListener(spark: SparkSession, job: BPStreamJob) extends BPStreamRemoteListener {
    import spark.implicits._
//    val jobTimestamp: mutable.Map[String, BPSEvents] = mutable.Map()
    def event2JobId(e: BPSEvents): String = e.jobId

    override def trigger(e: BPSEvents): Unit = {
        val jid = job.asInstanceOf[BPSJobContainer]
        e.`type` match {
            case "SandBox-Schema" => {
                val jid = job.asInstanceOf[BPSJobContainer]
                BPSOssPartitionMeta.pushLineToHDFS(jid.id, event2JobId(e), e.data)
                
            }
            case "SandBox-Length" => {
                BPSOssPartitionMeta.pushLineToHDFS(jid.id, event2JobId(e), e.data)
                post(s"""{"traceId": "${e.traceId}","jobId": "${e.jobId}"}""", "application/json")
                pollKafka(new FileMetaData(e.jobId, "/workData/streamingV2/" + jid.id + "/metadata/" + "",
                    "/workData/streamingV2/" + jid.id + "/files/" + "jobId=" + ""))
            }
        }
    }

    override def hit(e: BPSEvents): Boolean = e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Length"

    override def active(s: DataFrame): Unit = {
        BPSDriverChannel.registerListener(this)

        job.outputStream = s.filter($"type" === "SandBox-Schema" || $"type" === "SandBox-Length").writeStream
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
                .option("checkpointLocation", "/test/alex/" + UUID.randomUUID().toString + "/checkpoint")
                .start() :: job.outputStream
    }

    override def deActive(): Unit = {
        BPSDriverChannel.unRegisterListener(this)
    }

    def post(body: String, contentType: String): Unit = {
        val conn = new URL("http://192.168.100.116:36416/v0/UpdateJobIDWithTraceID").openConnection.asInstanceOf[HttpURLConnection]
        val postDataBytes = body.getBytes(StandardCharsets.UTF_8)
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", contentType)
        conn.setRequestProperty("Content-Length", String.valueOf(postDataBytes.length))
        conn.setConnectTimeout(60000)
        conn.setReadTimeout(60000)
        conn.setDoOutput(true)
        conn.getOutputStream.write(postDataBytes)
        conn.getResponseCode
    }

    def pollKafka(msg: FileMetaData): Unit ={
        //todo: 参数化
        val topic = "sb_file_meta_job_test"
        val pkp = new PharbersKafkaProducer[String, FileMetaData]
        val fu = pkp.produce(topic, msg.getJobId.toString, msg)
        println(fu.get(10, TimeUnit.SECONDS))
    }
}
