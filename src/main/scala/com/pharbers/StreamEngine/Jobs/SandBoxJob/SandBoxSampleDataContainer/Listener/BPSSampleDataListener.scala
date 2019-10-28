package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.Listener

import java.util.UUID

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

case class BPSSampleDataListener(spark: SparkSession, job: BPStreamJob) extends BPStreamRemoteListener{
	import spark.implicits._
	def event2JobId(e: BPSEvents): String = e.jobId
	
	override def trigger(e: BPSEvents): Unit = {
		e.`type` match {
			case "SandBox" =>
				val jid = job.asInstanceOf[BPSJobContainer]
				println(e.data)
				println(e.jobId)
				println(jid.id)
				// TODO 调用入库
			case _ =>
		}
	}
	
	override def active(s: DataFrame): Unit = {
		BPSDriverChannel.registerListener(this)
		job.outputStream = s.filter($"type" === "SandBox").writeStream
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
							"",
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
}

