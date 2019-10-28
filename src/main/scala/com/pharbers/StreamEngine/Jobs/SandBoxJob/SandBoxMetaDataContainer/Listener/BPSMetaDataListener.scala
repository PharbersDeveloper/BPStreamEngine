package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer.Listener

import java.util.UUID

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, ForeachWriter, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

case class BPSMetaDataListener(spark: SparkSession, job: BPStreamJob) extends BPStreamRemoteListener{
	import spark.implicits._
	def event2JobId(e: BPSEvents): String = e.jobId
	
	override def trigger(e: BPSEvents): Unit = {
		val jid = job.asInstanceOf[BPSJobContainer]
		println(e.data)
		
		// TODO: 调用入库接口
	}
	
	override def active(s: DataFrame): Unit = {
		BPSDriverChannel.registerListener(this)
		s.as[String].writeStream
			.foreach(
				new ForeachWriter[String] {
					var channel: Option[BPSWorkerChannel] = None
					def open(partitionId: Long, version: Long): Boolean = {
						if (channel.isEmpty) channel = Some(BPSWorkerChannel(TaskContext.get().getLocalProperty("host")))
						true
					}
					def process(value: String) : Unit = {
						// TODO 也许这边应该重新解析，测试了看一下
						implicit val formats = DefaultFormats
						val event = BPSEvents(
							"", "", "",
							value,
							null
						)
						channel.get.pushMessage(write(event))
					}
					def close(errorOrNull: scala.Throwable): Unit = {}//channel.get.close()
				}
			)
			.option("checkpointLocation", "/test/alex/" + UUID.randomUUID().toString + "/checkpoint")
			.start()
	}
	
	override def deActive(): Unit = {
		BPSDriverChannel.unRegisterListener(this)
	}
}

