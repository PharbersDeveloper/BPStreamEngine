package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleData.BPSSandBoxSampleDataJob
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object BPSSandBoxSampleDataJobContainer {
	def apply(jobId: String, spark: SparkSession): BPSSandBoxSampleDataJobContainer =
		new BPSSandBoxSampleDataJobContainer(jobId, spark)
}

class BPSSandBoxSampleDataJobContainer(jobId: String, override val spark: SparkSession) extends BPSJobContainer {
	//TODO 由调度器调度创建任务
	val id = UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	override val strategy = null
	
//	import spark.implicits._
	override def open(): Unit = {
		// TODO 先写死，后续策略读取返回流
		
//		val loadSchema =
//			StructType(
//				StructField("jobId", StringType) ::
//					StructField("traceId", StringType) ::
//					StructField("type", StringType) ::
//					StructField("data", StringType) ::
//					StructField("timestamp", TimestampType) :: Nil
//			)
//
//		this.inputStream = Some(spark.readStream
//			.schema(loadSchema)
//			.parquet(s"/test/alex/test000/files/jobId=$jobId"))
		
		this.inputStream = Some(spark.read.parquet(s"/test/alex/test001/files/jobId=$jobId"))
	}
	
	override def exec(): Unit = inputStream match {
		case Some(_) =>
			val job = BPSSandBoxSampleDataJob(id, spark, this.inputStream, this)
			job.exec()
			jobs += id -> job
			
		case None => ???
	}
}
