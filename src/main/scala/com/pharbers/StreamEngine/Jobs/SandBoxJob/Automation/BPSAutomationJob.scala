package com.pharbers.StreamEngine.Jobs.SandBoxJob.Automation

import java.util.concurrent.TimeUnit

import com.pharbers.StreamEngine.Utils.StreamJob.BPStreamJob
import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.kafka.producer.PharbersKafkaProducer
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object BPSAutomationJob {
	def apply(topic: String,
	          msg: SpecificRecord): BPSAutomationJob =
		new BPSAutomationJob(topic, msg)
}

class BPSAutomationJob(topic: String,
                       msg: SpecificRecord) extends BPStreamJob {
	val id: String = ""
	type T = BPSJobStrategy
	override val strategy: Null = null
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
