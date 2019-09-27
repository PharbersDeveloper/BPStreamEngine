package com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener

import com.pharbers.StreamEngine.BPJobChannels.DriverChannel.DriverChannel
import com.pharbers.StreamEngine.BPJobChannels.WorkerChannel.WorkerChannel
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.BPStreamJob.FileJobs.OssListener.OssEventsHandler.{BPSEndLengthHandler, BPSSchemaHandler}
import com.pharbers.StreamEngine.Common.Events
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamRemoteListener
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

case class BPSOssListener(val spark: SparkSession, val job: BPStreamJob) extends BPStreamRemoteListener {
//    var ins: Option[StreamingQuery] = None
    import spark.implicits._
    override def trigger(e: Events): Unit = {
        e.`type` match {
            case "SandBox-Schema" => BPSSchemaHandler().exec(job)(e)
            case "SandBox-Length" => BPSEndLengthHandler().exec(job)(e)
        }
    }

    override def hit(e: Events): Boolean = e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Length"

    override def active(s: DataFrame): Unit = {
        DriverChannel.registerListener(this)
        job.outputStream = s.filter($"type" === "SandBox-Schema").writeStream
            .foreach(
                new ForeachWriter[Row] {

                    var channel: Option[WorkerChannel] = None

                    def open(partitionId: Long, version: Long): Boolean = {
                        if (channel.isEmpty) channel = Some(WorkerChannel())
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
            ).start() :: job.outputStream
    }

    override def deActive(s: DataFrame): Unit = {
        DriverChannel.unRegisterListener(this)
    }
}
