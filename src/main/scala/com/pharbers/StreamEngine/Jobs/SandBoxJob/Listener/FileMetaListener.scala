package com.pharbers.StreamEngine.Jobs.SandBoxJob.Listener

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxMetaDataContainer.BPSSandBoxMetaDataJobContainer
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxSampleDataContainer.BPSSandBoxSampleDataJobContainer
import com.pharbers.kafka.consumer.PharbersKafkaConsumer
import com.pharbers.kafka.schema.FileMetaData
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FileMetaListener(spark: SparkSession, job: BPStreamJob) extends BPStreamRemoteListener {
	val pkc = new PharbersKafkaConsumer[String, FileMetaData]("sb_file_meta_job_test_1" :: Nil, 1000, Int.MaxValue, process)
	def process(record: ConsumerRecord[String, FileMetaData]): Unit = {
		println(record.value().getJobId)
		println(record.value().getMetaDataPath)
		println(record.value().getSampleDataPath)
		val mdJob = BPSSandBoxMetaDataJobContainer(record.value().getMetaDataPath.toString,
			                                       record.value().getJobId.toString, spark)
//		val sdJob = BPSSandBoxSampleDataJobContainer(record.value().getSampleDataPath.toString,
//			                                       record.value().getJobId.toString, spark)
		
		mdJob.open()
		mdJob.exec()
//		sdJob.open()
//		sdJob.exec()
	}
	override def trigger(e: BPSEvents): Unit = {}

	override def active(s: DataFrame): Unit = {
		BPSDriverChannel.registerListener(this)
		try {
			val t = new Thread(pkc)
			t.start()
		} catch {
			case e: Exception =>
				println(e.getMessage)
				pkc.close()
		}
	}

	override def deActive(): Unit = {
		BPSDriverChannel.unRegisterListener(this)
	}
}
