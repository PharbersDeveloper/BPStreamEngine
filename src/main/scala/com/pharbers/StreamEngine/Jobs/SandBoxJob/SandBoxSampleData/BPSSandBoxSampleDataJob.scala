package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleData

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

object BPSSandBoxSampleDataJob {
	def apply(id: String,
	          spark: SparkSession,
	          inputStream: Option[sql.DataFrame],
	          container: BPSJobContainer): BPSSandBoxSampleDataJob =
		new BPSSandBoxSampleDataJob(id, spark, container)
}

class BPSSandBoxSampleDataJob(val id: String,
                              val spark: SparkSession,
//                              val is: Option[sql.DataFrame],
                              val container: BPSJobContainer) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy = null
	import spark.implicits._
	
	override def exec(): Unit = {
//		inputStream = is
		inputStream match {
			case None =>
			case Some(is) =>
				is.show(false)
//				val traceIds = is.selectExpr("traceId")
//				val traceId = traceIds.first().getAs[String]("traceId")
//				val sampleData = is.selectExpr("data").toJSON.take(5).toList
//				BPFileMeta2Mongo(traceId, sampleData, "", 0).MetaData()
		
		}
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
}
