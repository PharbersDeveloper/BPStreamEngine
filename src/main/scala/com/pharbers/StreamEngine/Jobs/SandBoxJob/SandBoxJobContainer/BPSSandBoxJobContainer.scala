package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.Listener.FileMetaListener
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.BPSSandBoxSampleDataJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.BPSJobContainer
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSKfkJobStrategy
import org.apache.spark.sql.SparkSession

object BPSSandBoxJobContainer {
	def apply(spark: SparkSession): BPSSandBoxJobContainer =
		new BPSSandBoxJobContainer(spark)
}

class BPSSandBoxJobContainer( val spark: SparkSession) extends BPSJobContainer {
	// TODO 整体SandBoxJob的初始化
	val id =  UUID.randomUUID().toString //"1aed8-53d5-48f3-b7dd-780be0"
	type T = BPSKfkJobStrategy
	val strategy = null
	
	override def open(): Unit = {
	
	}
	
	override def exec(): Unit = {
		val listener = FileMetaListener(spark, this)
		listener.active(null)
		listeners = listener :: listeners
	}
}
