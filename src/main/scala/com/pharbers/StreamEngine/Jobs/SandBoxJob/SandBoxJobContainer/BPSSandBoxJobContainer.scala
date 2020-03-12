package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

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
	
	val id: String = ""//UUID.randomUUID().toString
	type T = BPSKfkJobStrategy
	val strategy: T  = null
	var sbcm: Option[BPSandBoxConsumerManager] = None
	override def open(): Unit = {
		// TODO log
		logger.info("初始化SandBoxJobContainer")
		if (sbcm.isEmpty) {
			sbcm = Some(BPSandBoxConsumerManager("sb_file_meta_job_k8s_test" :: Nil,spark))
		}
	}
	
	override def exec(): Unit = {
		// TODO log
		logger.info("执行SandBoxJobContainer")
		sbcm.get.exec()
	}
	
	override def registerListeners(listener: BPStreamListener): Unit = {}
	
	override def handlerExec(handler: BPSEventHandler): Unit = {}
	
	override def close(): Unit = {
		// TODO: Consumer关闭
		sbcm.get.close()
		super.close()
	}
}
