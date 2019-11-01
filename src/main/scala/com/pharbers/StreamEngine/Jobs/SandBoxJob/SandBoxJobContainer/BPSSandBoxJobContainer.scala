package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.Listener.BPKafkaJobListener
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.StreamJob.{BPDynamicStreamJob, BPSJobContainer}
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSSandBoxJobContainer {
	def apply(spark: SparkSession, config: Map[String, String]): BPSSandBoxJobContainer =
		new BPSSandBoxJobContainer(spark, config)
}

class BPSSandBoxJobContainer( val spark: SparkSession, config: Map[String, String])
	extends BPSJobContainer
		with BPDynamicStreamJob {
	
	val id: String = ""//UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy: T  = null
	
	override def open(): Unit = {
		// TODO log
		println("初始化SandBoxJobContainer")
	}
	
	override def exec(): Unit = {
		// TODO log
		println("执行SandBoxJobContainer")
		val job = BPKafkaJobListener(this.id, spark, this)
		job.exec()
	}
	
	override def registerListeners(listener: BPStreamListener): Unit = {}
	
	override def handlerExec(handler: BPSEventHandler): Unit = {}
}
