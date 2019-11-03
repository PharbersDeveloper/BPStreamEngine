package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.Listener

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataJob.BPSSandBoxMetaDataJob
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
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
	// TODO 多线程有问题，临时解决，或从源头解决
	var hisJobId = ""
	val process: ConsumerRecord[String, FileMetaData] => Unit = (record: ConsumerRecord[String, FileMetaData]) => {
		if (record.value().getJobId.toString != hisJobId) {
			hisJobId = record.value().getJobId.toString

			println("Fuck ===>" + record.value().getJobId)
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
			println("咋还重复传递JobID呢")
		}

		
//		if (record.value().getConvertType.toString == "convert_schema") {
//
//		}
	}
	
	override def exec(): Unit = {
		val pkc = new PharbersKafkaConsumer[String, FileMetaData](
			"sb_file_meta_job_test" :: Nil,
			1000,
			Int.MaxValue, process
		)
		try {
			val t = new Thread(pkc)
			t.start()
		} catch {
			case e: Exception =>
				println(e.getMessage)
				pkc.close()
		}
	}
	
	override def close(): Unit = {
		super.close()
		container.finishJobWithId(id)
	}
	
	
}
