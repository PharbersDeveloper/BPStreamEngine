package com.pharbers.StreamEngine.Jobs.EsJob.Listener

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

case class EsSinkJobCloseListener(id: String,
								  jobId: String,
								  spark: SparkSession,
								  job: BPStreamJob,
								  query: StreamingQuery,
								  sumRow: Long) extends BPStreamListener {
	override def trigger(e: BPSEvents): Unit = {
		val cumulative = query.recentProgress.map(_.numInputRows).sum

		if (cumulative >= sumRow) {
			logger.info(s"EsSinkJob(${jobId}) done with count = " + cumulative)
			// TODO 其他操作
			job.close()
		}
	}
	
	override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)

}
