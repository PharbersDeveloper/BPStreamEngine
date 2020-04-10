package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.Listener

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPStreamListener, BPStreamLocalListener}
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

//case class ConvertSchemaListener(id: String,
//                                 jobId: String,
//                                 spark: SparkSession,
//                                 job: BPStreamJob,
//                                 query: StreamingQuery,
//                                 sumRow: Long) extends BPStreamListener {
//	override def trigger(e: BPSEvents): Unit = {
//		val cumulative = query.recentProgress.map(_.numInputRows).sum
//
//		logger.debug(s"Listener number of records ========>$cumulative")
//		if (cumulative >= sumRow) {
//			logger.debug(s"ConvertSchemaListener End Of The Job，The total number of records ========>$cumulative")
//			job.close()
//		}
//	}
//
//	override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)
//
//	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
//}


case class ConvertSchemaListener(job: BPStreamJob, msgType: String) extends BPStreamLocalListener {
	def event2JobId(e: BPSEvents): String = e.jobId
	
	override def trigger(e: BPSEvents): Unit = {
//		val cumulative = query.recentProgress.map(_.numInputRows).sum

//		logger.debug(s"Listener number of records ========>$cumulative")
//		if (cumulative >= sumRow) {
//			logger.debug(s"ConvertSchemaListener End Of The Job，The total number of records ========>$cumulative")
//			job.close()
//		}
	}
	
	override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}
