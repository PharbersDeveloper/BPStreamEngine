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
			val jobId: String = UUID.randomUUID().toString
			val id: String = UUID.randomUUID().toString
			// TODO 路径配置化
			val metaDataSavePath: String = s"/jobs/$id/$jobId/metadata/"
			val checkPointSavePath: String = s"/jobs/$id/$jobId/checkpoint"
			val parquetSavePath: String =  s"/jobs/$id/$jobId/contents/"
			hisJobId = record.value().getJobId.toString
			
			val jobParam = Map(
				"parentJobId" -> record.value().getJobId.toString,
				"parentMetaData" -> record.value().getMetaDataPath.toString,
				"parentSampleData" -> record.value().getSampleDataPath.toString,
				"currentJobId" -> jobId,
				"metaDataSavePath" -> metaDataSavePath,
				"checkPointSavePath" -> checkPointSavePath,
				"parquetSavePath" -> parquetSavePath
			)
			
			val convertJob: BPSSandBoxConvertSchemaJob = BPSSandBoxConvertSchemaJob(
				record.value().getRunId.toString, jobParam, spark)
			convertJob.open()
			convertJob.exec()
			
			pushPyjob(
				id,
				s"$metaDataSavePath",
				s"$parquetSavePath" + jobId,
				jobId
			)
		} else {
			logger.error("咋还重复传递JobID呢", hisJobId)
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
		container.finishJobWithId(id)
	}
	
	// TODO: 老齐那边应该起一个kafka Listening，先暂时这样跑通
	private def pushPyjob(runId: String, metadataPath: String, filesPath: String, jobId: String): Unit ={
		import org.json4s._
		import org.json4s.jackson.Serialization.write
		implicit val formats: DefaultFormats.type = DefaultFormats
		//    val jobId = "201910231514"
		val traceId = ""
		val `type` = "add"
		val jobConfig = Map("jobId" -> jobId,
			"matedataPath" -> metadataPath,
			"filesPath" -> filesPath,
			"resultPath" -> "hdfs:///test/dcs/testPy2"
		)
		val job = JobMsg("ossPyJob" + jobId, "job", "com.pharbers.StreamEngine.Jobs.PyJob.PythonJobContainer.BPSPythonJobContainer",
			List("$BPSparkSession"), Nil, Nil, jobConfig, "", "test job")
		val jobMsg = write(job)
		val topic = "stream_job_submit"
		val pkp = new PharbersKafkaProducer[String, BPJob]
		val bpJob = new BPJob(jobId, traceId, `type`, jobMsg)
		val fu = pkp.produce(topic, jobId, bpJob)
		println(fu.get(10, TimeUnit.SECONDS))
	}
	
	def pollKafka(topic: String, msg: SpecificRecord, jobId: String): Unit ={
		//TODO: 参数化
		val pkp = new PharbersKafkaProducer[String, SpecificRecord]
		val fu = pkp.produce(topic, jobId, msg)
		logger.info(fu.get(10, TimeUnit.SECONDS))
	}
}
