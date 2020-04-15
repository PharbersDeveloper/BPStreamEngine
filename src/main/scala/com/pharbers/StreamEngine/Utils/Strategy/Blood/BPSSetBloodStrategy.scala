package com.pharbers.StreamEngine.Utils.Strategy.Blood

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import com.pharbers.kafka.schema.UploadEnd
import org.apache.kafka.common.config.ConfigDef
import org.apache.avro.specific.SpecificRecord

class BPSSetBloodStrategy (config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef()) {
	
	def getKafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]
	
	def pushBloodInfo(data: SpecificRecord, jobId: String, traceId: String, msgTyp: String = "SandBoxDataSet"): Unit = {
		getKafka.callKafka(BPSEvents(jobId, traceId, msgTyp, data))
	}
	
	def uploadEndPoint(uploadEnd: UploadEnd, jobId: String, traceId: String): Unit = {
		getKafka.callKafka(BPSEvents(jobId, traceId, "UploadEndPoint", uploadEnd))
	}
}
