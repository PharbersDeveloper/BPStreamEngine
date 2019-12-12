package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.Listener

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Utils.Component.Dynamic.JobMsg
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.producer.PharbersKafkaProducer
import com.pharbers.kafka.schema.{BPJob, FileMetaData}
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.mongodb.scala.bson.ObjectId

object BPKafkaJobListener {
	def apply(id: String, spark: SparkSession, container: BPSJobContainer): BPKafkaJobListener =
		new BPKafkaJobListener(id, spark, container)
}

class BPKafkaJobListener(val id: String,
                         val spark: SparkSession,
                         container: BPSJobContainer) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy: T = null
	var hisJobId = ""
	val process: ConsumerRecord[String, FileMetaData] => Unit = (record: ConsumerRecord[String, FileMetaData]) => {
		if (record.value().getJobId.toString != hisJobId) {
			val jobContainerId: String = UUID.randomUUID().toString
			val dataSetId = new ObjectId().toString
			hisJobId = record.value().getJobId.toString

			// TODO 路径配置化
//			val metaDataSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId"
//			val checkPointSavePath: String = s"/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
//			val parquetSavePath: String =  s"/jobs/${record.value().getRunId.toString}/$jobContainerId"

			val metaDataSavePath: String = s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/metadata"
			val checkPointSavePath: String = s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/checkpoint"
			val parquetSavePath: String =  s"/user/alex/jobs/${record.value().getRunId.toString}/$jobContainerId/contents"

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

			pushPyjob(
				record.value().getRunId.toString,
				s"$metaDataSavePath",
				s"$parquetSavePath",
				UUID.randomUUID().toString,
				(dataSetId :: Nil).mkString(",")
			)
		} else {
			logger.error("this is Repetitive Job", hisJobId)
		}
	}
	
	override def exec(): Unit = {
		val pkc = new PharbersKafkaConsumer[String, FileMetaData](
			"sb_file_meta_job" :: Nil,
			1000,
			Int.MaxValue, process
		)
		ThreadExecutor().execute(pkc)
	}
	
	override def close(): Unit = {
		super.close()
		// TODO: Consumer关闭
		container.finishJobWithId(id)
	}
	
	// TODO:  临时老齐那边应该起一个kafka Listening，先暂时这样跑通
	private def pushPyjob(runId: String,
	                      metadataPath: String,
	                      filesPath: String,
	                      parentJobId: String,
	                      dsIds: String): Unit = {
//		val resultPath = s"hdfs://jobs/$runId/"
		val resultPath = s"hdfs:///user/alex/jobs/$runId"
		
		import org.json4s._
		import org.json4s.jackson.Serialization.write
		implicit val formats: DefaultFormats.type = DefaultFormats
		val traceId = ""
		val `type` = "add"
		val jobConfig = Map(
			"jobId" -> parentJobId,
			"parentsOId" -> dsIds,
			"metadataPath" -> metadataPath,
			"filesPath" -> filesPath,
			"resultPath" -> resultPath
		)
		val job = JobMsg(
			s"ossPyJob$parentJobId",
			"job",
			"com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
			List("$BPSparkSession"),
			Nil,
			Nil,
			jobConfig,
			"",
			"temp job")
		val jobMsg = write(job)
		val topic = "stream_job_submit"
		val pkp = new PharbersKafkaProducer[String, BPJob]
		val bpJob = new BPJob(parentJobId, traceId, `type`, jobMsg)
		val fu = pkp.produce(topic, parentJobId, bpJob)
		logger.debug(fu.get(10, TimeUnit.SECONDS))
	}
}
