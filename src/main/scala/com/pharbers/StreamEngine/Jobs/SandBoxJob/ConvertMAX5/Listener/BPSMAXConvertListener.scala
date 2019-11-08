package com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5.Listener


import com.pharbers.StreamEngine.Jobs.SandBoxJob.ConvertMAX5.BPSConvertMAX5JobContainer
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

case class BPSMAXConvertListener(id: String,
                               jobId: String,
                               tailJobIds: List[(String ,String)],
                               spark: SparkSession,
                               job: BPStreamJob,
                               query: StreamingQuery,
                               sumRow: Long,
                               metaDataPath: String,
                               sampleDataPath: String) extends BPStreamListener {
	override def trigger(e: BPSEvents): Unit = {
		val cumulative = query.recentProgress.map(_.numInputRows).sum
		
		println("=========> Total Row " + sumRow)
		println("=====>" + cumulative)
		println("@@@@@@=>" + tailJobIds.size)
		if (cumulative >= sumRow) {
			println("******>" + cumulative)
			job.close()
			
			if (tailJobIds.nonEmpty) {
				val head = tailJobIds.head
				val tail = tailJobIds.tail
				val job2 = BPSConvertMAX5JobContainer(id, metaDataPath, sampleDataPath, head._2, head._1, tail, spark)
				job2.open()
				job2.exec()
			}
		}
	}
	
	override def active(s: DataFrame): Unit = BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}
