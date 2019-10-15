package com.pharbers.StreamEngine.Jobs.OssJob.OssListener

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.BPSOssPartitionJob
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Jobs.OssJob.OssListener.OssEventsHandler.{BPSEndLengthHandlerBPS, BPSSchemaHandlerBPS}
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

case class BPSOssListener(val spark: SparkSession, val job: BPStreamJob) extends BPStreamRemoteListener {
//    var ins: Option[StreamingQuery] = None
    import spark.implicits._
    override def trigger(e: BPSEvents): Unit = {
        e.`type` match {
            case "SandBox-Schema" => {
                val new_job = job.asInstanceOf[BPSJobContainer].getJobWithId(e.jobId)
                BPSSchemaHandlerBPS().exec(new_job)(e)
            }
            case "SandBox-Length" => {
                val new_job = job.asInstanceOf[BPSJobContainer].getJobWithId(e.jobId)
                BPSEndLengthHandlerBPS().exec(new_job)(e)
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
                            value.getAs[String]("data")
                        )
                        channel.get.pushMessage(write(event))
                    }

                    def close(errorOrNull: scala.Throwable): Unit = {}//channel.get.close()
                }
            )
                .option("checkpointLocation", "/test/streaming/" + UUID.randomUUID().toString + "/checkpoint")
                .start() :: job.outputStream
    }

    override def deActive(): Unit = {
        BPSDriverChannel.unRegisterListener(this)
    }
}
