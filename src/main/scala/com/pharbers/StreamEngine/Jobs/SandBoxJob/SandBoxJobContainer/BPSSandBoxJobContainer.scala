package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.Listener.FileMetaListener
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
	extends BPSJobContainer with BPDynamicStreamJob {
	// TODO 整体SandBoxJob的初始化
	val id =  UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy = null
	
	override def open(): Unit = {
		println("初始化SandBoxJob")
	}
	
	override def exec(): Unit = {
		println("执行SandBoxJob")
		val listener = FileMetaListener(spark, this)
		listener.active(null)
		listeners = listener :: listeners
	}
	
	override def registerListeners(listener: BPStreamListener): Unit = {}
	
	override def handlerExec(handler: BPSEventHandler): Unit = {}
}
