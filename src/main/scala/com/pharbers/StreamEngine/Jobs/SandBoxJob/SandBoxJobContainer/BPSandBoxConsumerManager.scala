package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.FileMetaData
import com.pharbers.util.log.PhLogable
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.mongodb.scala.bson.ObjectId
import org.apache.spark.sql.SparkSession

// TODO: 该类将管理Consumer并行与销毁，目前先实现单一为主
object BPSandBoxConsumerManager {
	def apply(topics: List[String],spark: SparkSession): BPSandBoxConsumerManager =
		new BPSandBoxConsumerManager(topics, spark)
}

class BPSandBoxConsumerManager(topics: List[String], spark: SparkSession) extends PhLogable {
	var hisJobId = ""
	var sandBoxConsumer: Option[PharbersKafkaConsumer[String, FileMetaData]] = None
	private val process: ConsumerRecord[String, FileMetaData] => Unit = (record: ConsumerRecord[String, FileMetaData]) => {
		if (record.value().getJobId.toString != hisJobId) {
			val jobContainerId: String = UUID.randomUUID().toString
			val dataSetId = new ObjectId().toString
			hisJobId = record.value().getJobId.toString
			
			// TODO 路径配置化
			val metaDataSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/metadata"
			val checkPointSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
			val parquetSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/contents"
			
//			val metaDataSavePath: String = s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/metadata"
//			val checkPointSavePath: String = s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
//			val parquetSavePath: String =  s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/contents"
			
			val jobParam = Map(
				"parentJobId" -> record.value().getJobId.toString,
				"parentMetaData" -> record.value().getMetaDataPath.toString,
				"parentSampleData" -> record.value().getSampleDataPath.toString,
				"jobContainerId" -> jobContainerId,
				"metaDataSavePath" -> metaDataSavePath,
				"checkPointSavePath" -> checkPointSavePath,
				"parquetSavePath" -> parquetSavePath
			)
			
			val convertJob: BPSSandBoxConvertSchemaJob =
				BPSSandBoxConvertSchemaJob(
					record.value().getRunId.toString,
					jobParam,
					spark,
					dataSetId)
			convertJob.open()
			convertJob.exec()
		} else {
			logger.error("this is repetitive job", hisJobId)
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
	}
	
	def close(): Unit = {
		if (sandBoxConsumer.isDefined) {
			sandBoxConsumer.get.close()
		} else {
			logger.warn("Consumer is None")
		}
	}
	
}
