package com.pharbers.StreamEngine.Utils.Strategy.Blood

import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.kafka.schema.UploadEnd
import org.apache.kafka.common.config.ConfigDef
import org.apache.avro.specific.SpecificRecord

class BPSSetBloodStrategy (config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef()) {
	
	val strategy: BPSCommonJobStrategy =  BPSCommonJobStrategy(config, inoutConfigDef)
	
	def pushBloodInfo(data: SpecificRecord, jobId: String, traceId: String, msgTyp: String = "SandBoxDataSet"): Unit = {
		strategy.pushMsg(BPSEvents(jobId, traceId, msgTyp, data), isLocal = false)
	}
	
	def uploadEndPoint(uploadEnd: UploadEnd, jobId: String, traceId: String): Unit = {
		strategy.pushMsg(BPSEvents(jobId, traceId, "UploadEndPoint", uploadEnd), isLocal = false)
	}
}
