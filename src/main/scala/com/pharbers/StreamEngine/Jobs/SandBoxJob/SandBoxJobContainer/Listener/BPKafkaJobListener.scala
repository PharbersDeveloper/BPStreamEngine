package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.Listener

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataJob.BPSSandBoxMetaDataJob
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.FileMetaData
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
			hisJobId = record.value().getJobId.toString

			BPSSandBoxMetaDataJob(record.value().getMetaDataPath.toString,
				record.value().getJobId.toString, spark).exec()

			//			val sdJob = BPSSandBoxSampleDataJobContainer(record.value().getSampleDataPath.toString,
			//				record.value().getJobId.toString, spark)
			//			sdJob.open()
			//			sdJob.exec()

			val convertJob: BPSSandBoxConvertSchemaJob = BPSSandBoxConvertSchemaJob(
				record.value().getRunId.toString,
				record.value().getMetaDataPath.toString,
				record.value().getSampleDataPath.toString,
				record.value().getJobId.toString, spark)
			convertJob.open()
			convertJob.exec()
		} else {
			logger.error("咋还重复传递JobID呢", hisJobId)
		}
	}
	
	override def exec(): Unit = {
		val pkc = new PharbersKafkaConsumer[String, FileMetaData](
			"sb_file_meta_job_test" :: Nil,
			1000,
			Int.MaxValue, process
		)
		ThreadExecutor().execute(pkc)
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
	
	
}
