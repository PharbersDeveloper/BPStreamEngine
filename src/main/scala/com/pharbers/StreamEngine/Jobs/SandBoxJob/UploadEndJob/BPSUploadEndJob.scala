package com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object BPSUploadEndJob {
	def apply(topic: String,
	          msg: SpecificRecord): BPSUploadEndJob =
		new BPSUploadEndJob(topic, msg)
}

// TODO：后续改为一个函数即可，无需使用Job
class BPSUploadEndJob(topic: String,
                  msg: SpecificRecord) extends BPStreamJob {
	val id: String = ""
	type T = BPSJobStrategy
	override val strategy = null
	val is: Option[sql.DataFrame] = None
	val spark: SparkSession = null
	
	override def exec(): Unit = {
		val producerInstance = new PharbersKafkaProducer[String, SpecificRecord]
		val fu = producerInstance.produce(topic, id, msg)
//		val fu = ProducerSingleton.getIns.produce(topic, id, msg)
		logger.debug(fu.get(10, TimeUnit.SECONDS))
		producerInstance.producer.close()
	}
	
	override def close(): Unit = {
		super.close()
	}
}
