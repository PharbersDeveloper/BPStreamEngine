package com.pharbers.StreamEngine.Jobs.SandBoxJob.BloodJob

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.StreamJob.{BPSJobContainer, BPStreamJob}
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

object BPSBloodJob {
	def apply(jobId: String,
	          topic: String,
	          msg: SpecificRecord): BPSBloodJob =
		new BPSBloodJob(jobId, topic, msg)
}

class BPSBloodJob(val id: String,
                  topic: String,
                  msg: SpecificRecord) extends BPStreamJob {
	type T = BPSJobStrategy
	override val strategy = null
	val is: Option[sql.DataFrame] = None
	val spark: SparkSession = null
	
	override def exec(): Unit = {
		val fu = new PharbersKafkaProducer[String, SpecificRecord].produce(topic, id, msg)
		logger.debug(fu.get(10, TimeUnit.SECONDS))
	}
	
	override def close(): Unit = {
		super.close()
	}
}
