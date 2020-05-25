package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.concurrent.atomic.AtomicInteger
import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Annotation.Component
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Component2
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.{BPJobLocalListener, BPJobRemoteListener, BPStreamListener}
import com.pharbers.StreamEngine.Utils.Event.msgMode.FileMetaData
import com.pharbers.StreamEngine.Utils.Job.{BPDynamicStreamJob, BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.collection.mutable

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
	val strategy: BPSCommonJobStrategy = BPSCommonJobStrategy(componentProperty.config, configDef)
	val queue = new mutable.Queue[BPSTypeEvents[FileMetaData]]
	val execQueueJob = new AtomicInteger(0)
	val id: String = componentProperty.id
	override val jobId: String = strategy.getJobId
	val localChanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel]

	override val spark: SparkSession = strategy.getSpark
	
	override def open(): Unit = {
		logger.info("Open SandBoxJobContainer")
		queueListener()
	}
	
	override def exec(): Unit = {
		logger.info("Exec Listener")
		
		val listenEvent: Seq[String] = strategy.getListens
		val listener: BPJobRemoteListener[FileMetaData] =
			BPJobRemoteListener[FileMetaData](this, listenEvent.toList)(x => starJob(x))
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
	
	def starJob(event: BPSTypeEvents[FileMetaData]): Unit = {
		if(!strategy.getS3aFile.checkPath(event.data.sampleDataPath)){
			strategy.getS3aFile.appendLine(event.data.sampleDataPath + "/_SUCCESS","")
		}

		queue.enqueue(event)
		
		if (execQueueJob.get() < componentProperty.config("queue").toInt) {
			runJob()
		} else {
			logger.warn("queue is full")
		}
	}
	
	def runJob(): Unit ={
		if (queue.nonEmpty) {
			execQueueJob.incrementAndGet()
			val jobParameter = queue.dequeue()
			val reading = spark.readStream
				.schema(StructType(
					StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) :: Nil
				)).parquet(jobParameter.data.sampleDataPath)
			
			val pythonMsgType: String = strategy.jobConfig.getString(FILE_MSG_TYPE_KEY)
			val job = BPSSandBoxConvertSchemaJob(this, Some(reading), BPSComponentConfig(jobParameter.data.id,
				"BPSSandBoxConvertSchemaJob",
				jobParameter.traceId :: pythonMsgType :: Nil,
				Map("jobId" -> jobParameter.data.jobId)))
			
			jobs += job.id -> job
			try {
				job.open()
				job.exec()
			} catch {
				case e: Exception => logger.error(e.getMessage); job.close()
			}
		}
	}
	
	def queueListener(): Unit = {
		val jobEndListener: BPJobRemoteListener[String] =
			BPJobRemoteListener[String](null, List(s"SandBoxJobEnd"))(_ => {
				execQueueJob.decrementAndGet()
				logger.debug("################ execQueueJob Size =====> " + execQueueJob.get())
				runJob()
			})
		jobEndListener.active(null)
		
//		val jobEndListener = BPJobLocalListener[String](null, List(s"SandBoxJobEnd"))(_ => {
//			execQueueJob.decrementAndGet()
//			println("################ execQueueJob Size =====> " + execQueueJob.get())
//			runJob()
//		})
//		jobEndListener.active(null)
	}
}

