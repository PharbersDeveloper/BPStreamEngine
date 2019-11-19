package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.Listener

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.BPFileMeta2Mongo
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BPSSampleDataListener(spark: SparkSession, job: BPStreamJob, jobId: String, qv: String)
	extends BPStreamRemoteListener {
	
	override def trigger(e: BPSEvents): Unit = {
		// TODO 搞不懂还不会
//		if (sq.lastProgress != null) {
//			println("====>" + sq.lastProgress.numInputRows)
//		}

		val tmp = spark.sql(s"select * from $qv limit 5")
    		.selectExpr("data")
			.collect().map(x => x.toString().replaceAll("""\\"""", ""))
			.toList
		if (tmp.nonEmpty) {
			BPFileMeta2Mongo(jobId, tmp, "", 0).SampleData()
			spark.catalog.dropTempView(qv)
			job.close()
		}
	}
	
	override def active(s: DataFrame): Unit =  BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}

