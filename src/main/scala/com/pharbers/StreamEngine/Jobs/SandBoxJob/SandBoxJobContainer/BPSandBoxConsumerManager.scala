package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.FileMetaData
import com.pharbers.util.log.PhLogable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mongodb.scala.bson.ObjectId
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import scala.collection.mutable

object BPSandBoxConsumerManager {
	def apply(topics: List[String],spark: SparkSession): BPSandBoxConsumerManager =
		new BPSandBoxConsumerManager(topics, spark)
}

class BPSandBoxConsumerManager(topics: List[String], spark: SparkSession) extends PhLogable {
	var hisJobId = ""
	var hisSampleDataPath = ""
	var reading: Option[org.apache.spark.sql.DataFrame] = None
	var sandBoxConsumer: Option[PharbersKafkaConsumer[String, FileMetaData]] = None
	val jobQueue = new mutable.Queue[Map[String, String]]
	val maxQueueJob = 3
	var execQueueJob = new AtomicInteger(0)
	
	private val process: ConsumerRecord[String, FileMetaData] => Unit = (record: ConsumerRecord[String, FileMetaData]) => {
		if (record.value().getJobId.toString != hisJobId) {
			val jobContainerId: String = UUID.randomUUID().toString
			hisJobId = record.value().getJobId.toString
			
			// TODO 路径配置化
			val metaDataSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/metadata"
			val checkPointSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
			val parquetSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/contents"
			
			val jobParam = Map(
				"runId" -> record.value().getRunId.toString,
				"parentJobId" -> record.value().getJobId.toString,
				"parentMetaData" -> record.value().getMetaDataPath.toString,
				"parentSampleData" -> record.value().getSampleDataPath.toString,
				"jobContainerId" -> jobContainerId,
				"metaDataSavePath" -> metaDataSavePath,
				"checkPointSavePath" -> checkPointSavePath,
				"parquetSavePath" -> parquetSavePath,
				"dataSetId" ->  new ObjectId().toString
			)
			// TODO 无界队列，有危险
			jobQueue.enqueue(jobParam)
			
			logger.info(s"ParentJobId ======> ${record.value().getJobId.toString}")
			
			if (record.value().getSampleDataPath.toString != hisSampleDataPath) {
				 hisSampleDataPath = record.value().getSampleDataPath.toString
				 reading = Some(spark.readStream
					//todo: 控制文件大小，使后序流不至于一次读取太多文件，效果待测试
	                .option("maxFilesPerTrigger", 10)
					.schema(StructType(
						StructField("traceId", StringType) ::
						StructField("type", StringType) ::
						StructField("data", StringType) ::
						StructField("timestamp", TimestampType) ::
						StructField("jobId", StringType) :: Nil
					))
					.parquet(s"${jobParam("parentSampleData")}"))
				logger.info("Init reading")
			}
		} else {
			logger.error("this is repetitive job", hisJobId)
		}
	}
	
	def execQueue(): Unit = {
		while (true) {
			logger.info(s"jobQueue Num =====> ${jobQueue.length}")
			if (execQueueJob.get() <= maxQueueJob && jobQueue.nonEmpty) {
				val parm = jobQueue.dequeue()
				execQueueJob.incrementAndGet()
				val convertJob: BPSSandBoxConvertSchemaJob =
					BPSSandBoxConvertSchemaJob(
						parm("runId"),
						parm,
						spark,
						reading,
						execQueueJob)
				convertJob.open()
				convertJob.exec()
			}
			Thread.sleep(1 * 1000)
		}
	}
	
	def exec(): Unit = {
		val pkc = new PharbersKafkaConsumer[String, FileMetaData](
			topics,
			1000,
			Int.MaxValue, process
		)
		ThreadExecutor().execute(pkc)
		sandBoxConsumer = Some(pkc)
		execQueue()
	}
	
	def close(): Unit = {
		if (sandBoxConsumer.isDefined) {
			sandBoxConsumer.get.close()
		} else {
			logger.warn("Consumer is None")
		}
	}
	
}
