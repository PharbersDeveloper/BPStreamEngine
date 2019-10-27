package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSandBoxJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SBListener.BPSBListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

object BPSSandBoxJobContainer {
	def apply(strategy: BPSKfkJobStrategy, spark: SparkSession): BPSSandBoxJobContainer =
		new BPSSandBoxJobContainer(strategy, spark)
}

class BPSSandBoxJobContainer(override val strategy: BPSKfkJobStrategy, val spark: SparkSession) extends BPSJobContainer {
	// TODO 整体SandBoxJob的初始化
	val id =  UUID.randomUUID().toString //"1aed8-53d5-48f3-b7dd-780be0"
	type T = BPSKfkJobStrategy
	import spark.implicits._
//	override def open(): Unit = {
////		val reading = spark
////			.read
////			.parquet("/test/alex/test000/files/jobId=1aed8-53d5-48f3-b7dd-780be0")
//		val reading = spark
//			.read
//			.text("/test/alex/test000/metadata/1aed8-53d5-48f3-b7dd-780be0")
//
//		inputStream = Some(reading)
//	}
//	override def exec(): Unit = inputStream match {
//		case Some(_) => {
//			// TODO：加入Kafka的任务中监控任务调度执行，下一步
////			val listener = BPSBListener(spark, this)
////			listener.active(is)
////			listeners = listener :: listeners
//
//			val job = BPSandBoxJob(id, spark, this.inputStream, this)
//			job.exec()
//			jobs += id -> job
//		}
//		case None => ???
//	}
	
	override def open(): Unit = {
		// TODO 初始化Kafka策略
		
	}
	
	override def exec(): Unit = inputStream match {
		case Some(_) =>
		case None =>
	}
	
	override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
		jobs.get(id) match {
			case Some(job) => job
			case None =>
				val job = BPSandBoxJob(id, spark, this.inputStream, this)
				job.exec()
				jobs += id -> job
				job
		}
	}
}
