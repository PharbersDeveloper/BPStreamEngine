package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener

import java.util.UUID

import com.pharbers.StreamEngine.BPJobChannels.DriverChannel.DriverChannel
import com.pharbers.StreamEngine.BPJobChannels.WorkerChannel.WorkerChannel
import com.pharbers.StreamEngine.BPStreamJob.BPSJobContainer.BPSJobContainer
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.BPSOssJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.OssEventsHandler.{BPSEndLengthHandler, BPSSchemaHandler}
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamRemoteListener
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

case class BPSOssListener(val spark: SparkSession, val job: BPStreamJob) extends BPStreamRemoteListener {
//    var ins: Option[StreamingQuery] = None
    import spark.implicits._
    override def trigger(e: Events): Unit = {
        e.`type` match {
            case "SandBox-Schema" => {
                val new_job = job.asInstanceOf[BPSJobContainer].getJobWithId(e.jobId)
                BPSSchemaHandler().exec(new_job)(e)
            }
            case "SandBox-Length" => {
                val new_job = job.asInstanceOf[BPSJobContainer].getJobWithId(e.jobId)
                BPSEndLengthHandler().exec(new_job)(e)
            }
        }
    }

    override def hit(e: Events): Boolean = e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Length"

    override def active(s: DataFrame): Unit = {
        DriverChannel.registerListener(this)

        job.outputStream = s.filter($"type" === "SandBox-Schema" || $"type" === "SandBox-Length").writeStream
            .foreach(
                new ForeachWriter[Row] {

                    var channel: Option[WorkerChannel] = None

                    def open(partitionId: Long, version: Long): Boolean = {
                        if (channel.isEmpty) channel = Some(WorkerChannel(TaskContext.get().getLocalProperty("host")))
                        true
                    }

                    def process(value: Row) : Unit = {

                        implicit val formats = DefaultFormats

                        val event = Events(
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
        DriverChannel.unRegisterListener(this)
    }
}
