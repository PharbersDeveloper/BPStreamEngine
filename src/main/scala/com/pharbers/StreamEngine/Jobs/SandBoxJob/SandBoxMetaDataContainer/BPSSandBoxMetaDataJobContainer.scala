package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer.Listener.BPSMetaDataListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSSandBoxMetaDataJobContainer {
	def apply(path: String,
	          jobId: String,
	          spark: SparkSession): BPSSandBoxMetaDataJobContainer =
		new BPSSandBoxMetaDataJobContainer(path, jobId, spark)
}

class BPSSandBoxMetaDataJobContainer(path: String,
                                     jobId: String,
                                     override val spark: SparkSession) extends BPSJobContainer {
	//TODO 由调度器调度创建任务
	val id  = UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy = null
	
	override def open(): Unit = {
		this.inputStream = Some(spark.readStream
			.format("text")
			.option("path", s"$path")
			.load())
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) =>
			val listener = BPSMetaDataListener(spark, this)
			listener.active(is)
			listeners = listener :: listeners
		
		case None => ???
	}
}
