
//TODO 重构完这个将删除，现在这个无用
//package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer
//
//import java.util.UUID
//import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
//
//import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSSandBoxConvertSchemaJob
//import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
//import com.pharbers.kafka.consumer.PharbersKafkaConsumer
//import com.pharbers.kafka.schema.FileMetaData
//import com.pharbers.util.log.PhLogable
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.mongodb.scala.bson.ObjectId
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
//
//import scala.collection.mutable
//
//object BPSandBoxConsumerManager {
//	def apply(topics: List[String],spark: SparkSession): BPSandBoxConsumerManager =
//		new BPSandBoxConsumerManager(topics, spark)
//}
//
//class BPSandBoxConsumerManager(topics: List[String], spark: SparkSession) extends PhLogable {
//	var hisJobId = ""
//	var hisSampleDataPath = ""
//	var reading: Option[org.apache.spark.sql.DataFrame] = None
//	var sandBoxConsumer: Option[PharbersKafkaConsumer[String, FileMetaData]] = None
//	val execQueueIns: Queue = Queue(spark)
//
//	private val process: ConsumerRecord[String, FileMetaData] => Unit = (record: ConsumerRecord[String, FileMetaData]) => {
//		if (record.value().getJobId.toString != hisJobId) {
//			val jobContainerId: String = UUID.randomUUID().toString
//			hisJobId = record.value().getJobId.toString
//
//			// TODO 路径配置化
//			val metaDataSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/metadata"
//			val checkPointSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
//			val parquetSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/contents"
//
//			val jobParam = Map(
//				"runId" -> record.value().getRunId.toString,
//				"parentJobId" -> record.value().getJobId.toString,
//				"parentMetaData" -> record.value().getMetaDataPath.toString,
//				"parentSampleData" -> record.value().getSampleDataPath.toString,
//				"jobContainerId" -> jobContainerId,
//				"metaDataSavePath" -> metaDataSavePath,
//				"checkPointSavePath" -> checkPointSavePath,
//				"parquetSavePath" -> parquetSavePath,
//				"dataSetId" ->  new ObjectId().toString
//			)
//			logger.info(s"ParentJobId ======> ${record.value().getJobId.toString}")
//
//			if (record.value().getSampleDataPath.toString != hisSampleDataPath) {
//				 hisSampleDataPath = record.value().getSampleDataPath.toString
//				 reading = Some(spark.readStream
//					//todo: 控制文件大小，使后序流不至于一次读取太多文件，效果待测试
//	                .option("maxFilesPerTrigger", 10)
//					.schema(StructType(
//						StructField("traceId", StringType) ::
//						StructField("type", StringType) ::
//						StructField("data", StringType) ::
//						StructField("timestamp", TimestampType) ::
//						StructField("jobId", StringType) :: Nil
//					))
//					.parquet(s"${jobParam("parentSampleData")}"))
//				logger.info("Init reading")
//			}
//			execQueueIns.reading = reading
//			// TODO 无界队列，有危险
//			execQueueIns.jobQueue.enqueue(jobParam)
//		} else {
//			logger.error("this is repetitive job", hisJobId)
//		}
//	}
//
//	def exec(): Unit = {
//		val pkc = new PharbersKafkaConsumer[String, FileMetaData](
//			topics,
//			1000,
//			Int.MaxValue, process
//		)
//		ThreadExecutor().execute(pkc)
//		sandBoxConsumer = Some(pkc)
//		ThreadExecutor().execute(execQueueIns)
//
//	}
//
//	def close(): Unit = {
//		if (sandBoxConsumer.isDefined) {
//			sandBoxConsumer.get.close()
//		} else {
//			logger.warn("Consumer is None")
//		}
//	}
//
//}
