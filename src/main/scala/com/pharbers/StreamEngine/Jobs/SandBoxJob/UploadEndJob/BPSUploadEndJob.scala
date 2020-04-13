//package com.pharbers.StreamEngine.Jobs.SandBoxJob.UploadEndJob
//
//import java.util.concurrent.TimeUnit
//
//import com.pharbers.StreamEngine.Utils.Component2
//import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
//import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
//import com.pharbers.kafka.producer.PharbersKafkaProducer
//import org.apache.avro.specific.SpecificRecord
//import org.apache.kafka.common.config.ConfigDef
//import org.apache.spark.sql
//import org.apache.spark.sql.SparkSession
//
//// TODO  等重构
//@deprecated
//object BPSUploadEndJob {
//	def apply(topic: String,
//	          msg: SpecificRecord): BPSUploadEndJob =
//		new BPSUploadEndJob(topic, msg)
//}
//
//// TODO：后续改为一个函数即可，无需使用Job
//@deprecated
//class BPSUploadEndJob(topic: String,
//                  msg: SpecificRecord) extends BPStreamJob {
//	val id: String = ""
//	override val description: String = "upload_end"
//	type T = BPStrategyComponent
//	override val strategy = null
//	val is: Option[sql.DataFrame] = None
//	val spark: SparkSession = null
//
//	override def exec(): Unit = {
//		val producerInstance = new PharbersKafkaProducer[String, SpecificRecord]
//		val fu = producerInstance.produce(topic, id, msg)
////		val fu = ProducerSingleton.getIns.produce(topic, id, msg)
//		logger.debug(fu.get(10, TimeUnit.SECONDS))
//		producerInstance.producer.close()
//	}
//
//	override def close(): Unit = {
//		super.close()
//	}
//
//	override val componentProperty: Component2.BPComponentConfig = null
//
//	override def createConfigDef(): ConfigDef = ???
//}
