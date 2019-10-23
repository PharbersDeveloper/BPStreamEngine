package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSandBoxJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.FileMeta2Mongo.SBListener.BPSBListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

object BPSSandBoxJobContainer {
	def apply(spark: SparkSession): BPSSandBoxJobContainer =
		new BPSSandBoxJobContainer(spark)
}

class BPSSandBoxJobContainer(val spark: SparkSession) extends BPSJobContainer {
	val id = "test000"
	type T = BPSKfkJobStrategy
	val strategy: BPSKfkJobStrategy = null
//	import spark.implicits._
	
	override def open(): Unit = {
		val reading = spark
			.read
			.load("/test/alex/test000/metadata/1aed8-53d5-48f3-b7dd-780be0")
		// /test/alex/test000/metadata/1aed8-53d5-48f3-b7dd-780be0 //不是HDFS文件 干还要像个办法兼容
		// /test/alex/test000/files/jobId=1aed8-53d5-48f3-b7dd-780be0 // HDFS
		
		inputStream = Some(reading)
	}
	
	override def exec(): Unit = inputStream match {
		case Some(_) => {
//			val listener = BPSBListener(spark, this)
//			listener.active(is)
//			listeners = listener :: listeners
			
			val job = BPSandBoxJob(id, spark, this.inputStream, this)
			job.exec()
			jobs += id -> job
		}
		case None => ???
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
