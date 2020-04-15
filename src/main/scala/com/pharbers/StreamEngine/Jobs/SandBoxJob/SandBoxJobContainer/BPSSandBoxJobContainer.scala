package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.BPSTypeEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}


object BPSSandBoxJobContainer {
	def apply(componentProperty: Component2.BPComponentConfig): BPSSandBoxJobContainer =
		new BPSSandBoxJobContainer(componentProperty)
}

@Component(name = "BPSSandBoxJobContainer", `type` = "BPSSandBoxJobContainer")
class BPSSandBoxJobContainer(override val componentProperty: Component2.BPComponentConfig)
	extends BPSJobContainer with BPDynamicStreamJob {
	
	final private val FILE_MSG_TYPE_KEY = "Python.msgType"
	final private val FILE_MSG_TYPE_DOC = "push Python msg type"
	final private val FILE_MSG_TYPE_DEFAULT = "Python.msgType"
	
	
	val description: String = "SandBox Start"
	type T = BPSCommonJobStrategy
	val strategy = BPSCommonJobStrategy(componentProperty.config, configDef)
	val id: String = componentProperty.id
	val jobId: String = strategy.getJobId
	
	var hisRunnerId = ""
	
	override val spark: SparkSession = strategy.getSpark
	
	override def open(): Unit = {
		logger.info("Open SandBoxJobContainer")
	}
	
	override def exec(): Unit = {
		logger.info("Exec Listener")
		
		val listenEvent: Seq[String] = strategy.getListens
		val listener: BPJobRemoteListener[Map[String, String]] =
			BPJobRemoteListener[Map[String, String]](this, listenEvent.toList)(x => starJob(x))
		listener.active(null)
		listeners = listener +: listeners
	}
	
	override def getJobWithId(id: String, category: String = ""): BPStreamJob = {
		jobs.get(id) match {
			case Some(job) => job
			case None =>
				val job = category match {
					case _ => ???
				}
				jobs += id -> job
				job
		}
	}
	
	override def registerListeners(listener: BPStreamListener): Unit = {}
	
	override def handlerExec(handler: BPSEventHandler): Unit = {}
	
	override def createConfigDef(): ConfigDef = {
		new ConfigDef().define (
			FILE_MSG_TYPE_KEY,
			Type.STRING,
			FILE_MSG_TYPE_DEFAULT,
			Importance.HIGH,
			FILE_MSG_TYPE_DOC
		)
	}
	
	def starJob(event: BPSTypeEvents[Map[String, String]]): Unit = {
		
		// TODO 这里有问题，我先测试一下，然后删除代码
		if (hisRunnerId != BPSConcertEntry.runner_id) {
			val reading = spark.readStream
				.option("maxFilesPerTrigger", 10)
				.schema(StructType(
					StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) ::
						StructField("jobId", StringType) :: Nil
				)).parquet(event.date.getOrElse("sampleDataPath", ""))
			inputStream = Some(reading)
			
			hisRunnerId = BPSConcertEntry.runner_id
		}
		val pythonMsgType: String = strategy.jobConfig.getString(FILE_MSG_TYPE_KEY)
		val job = BPSSandBoxConvertSchemaJob(this, BPSComponentConfig(UUID.randomUUID().toString,
				"BPSSandBoxConvertSchemaJob",
				event.traceId :: pythonMsgType :: Nil,
				event.date))
		jobs += job.id -> job
		logger.info(s"Job ${job.id} BPSSandBoxConvertSchemaJob =====> open and exec")
		try {
			job.open()
			job.exec()
		} catch {
			case e: Exception => logger.error(e.getMessage, e)
				job.close()
		}

	}
}