package com.pharbers.StreamEngine.OssJobs.FileJobs.OssListenerV2

import java.util.UUID

import com.pharbers.StreamEngine.BPJobChannels.DriverChannel.DriverChannel
import com.pharbers.StreamEngine.BPJobChannels.WorkerChannel.WorkerChannel
import com.pharbers.StreamEngine.OssJobs.BPSJobContainer.BPSJobContainer
import com.pharbers.StreamEngine.BPStreamJob.BPStreamJob
import com.pharbers.StreamEngine.Common.Event.BPSEvents
import com.pharbers.StreamEngine.OssJobs.FileJobs.OssListenerV2.OssEventsHandler.BPSSchemaHandlerV2BPS
import com.pharbers.StreamEngine.Common.Event.StreamListener.BPStreamRemoteListener
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

import scala.collection.mutable

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/11 13:30
  * @note 一些值得注意的地方
  */
case class BPSOssListenerV2(spark: SparkSession, job: BPStreamJob) extends BPStreamRemoteListener {
    import spark.implicits._
    val jobTimestamp: mutable.Map[String, BPSEvents] = mutable.Map()
    override def trigger(e: BPSEvents): Unit = {
        e.`type` match {
            case "SandBox-Schema" => jobTimestamp.put(e.jobId, e)
            case "SandBox-Length" => {
                val new_job = job.asInstanceOf[BPSJobContainer].getJobWithId(e.jobId)
                BPSSchemaHandlerV2BPS(jobTimestamp(e.jobId)).exec(new_job)(e)
                jobTimestamp.remove(e.jobId)
            }
        }
    }

    override def hit(e: BPSEvents): Boolean = e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Length"

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
                .option("checkpointLocation", "/test/streaming/" + UUID.randomUUID().toString + "/checkpoint")
                .start() :: job.outputStream
    }

    override def deActive(): Unit = {
        DriverChannel.unRegisterListener(this)
    }
}
