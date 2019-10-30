package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.Listener

import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.BPSMongo.BPFileMeta2Mongo
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import org.apache.spark.sql.{DataFrame, SparkSession}

case class BPSSampleDataListener(spark: SparkSession, job: BPStreamJob, qv: String, jobId: String)
	extends BPStreamRemoteListener {
	
	override def trigger(e: BPSEvents): Unit = {
		val tmp = spark.sql(s"select * from $qv")
    		.selectExpr("data")
			.collect().map(x => x.toString().replaceAll("""\\"""", ""))
			.toList.take(5)
		if (tmp.nonEmpty) {
			BPFileMeta2Mongo(jobId, tmp, "", 0).SampleData()
			spark.catalog.dropTempView(qv)
//			job.close()
		}
	}
	
	override def active(s: DataFrame): Unit =  BPSLocalChannel.registerListener(this)
	
	override def deActive(): Unit = BPSLocalChannel.unRegisterListener(this)
}

