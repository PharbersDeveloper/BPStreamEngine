package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaData.BPSSandBoxMetaDataJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer.Listener.BPSMetaDataListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSSandBoxMetaDataJobContainer {
	def apply(jobId: String, spark: SparkSession): BPSSandBoxMetaDataJobContainer =
		new BPSSandBoxMetaDataJobContainer(jobId, spark)
}

class BPSSandBoxMetaDataJobContainer(jobId: String, override val spark: SparkSession) extends BPSJobContainer {
	//TODO 由调度器调度创建任务
	val id: String = UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	override val strategy: Null = null
	
	override def open(): Unit = {
		// TODO 先写死，后续策略读取返回流
		
//		this.inputStream = Some(spark.read.text(s"/test/alex/test001/metadata/$jobId"))
		this.inputStream = Some(spark.readStream.text(s"/test/alex/test001/metadata/$jobId"))
	}
	
	override def exec(): Unit = inputStream match {
		case Some(is) =>
			val listener = BPSMetaDataListener(spark, this)
			listener.active(is)
			listeners = listener :: listeners
			
//			val job = BPSSandBoxMetaDataJob(id, spark, this.inputStream, this)
//			job.exec()
//			jobs += id -> job
		
		case None => ???
	}
}
