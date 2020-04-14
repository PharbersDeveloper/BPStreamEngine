package com.pharbers.StreamEngine.Utils.Strategy.Blood

import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.JobStrategy.BPSCommonJobStrategy
import com.pharbers.kafka.schema.{DataSet, UploadEnd}
import org.apache.kafka.common.config.ConfigDef

class BPSSetBloodStrategy (config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef())
	extends BPSCommonJobStrategy(config, inoutConfigDef) {
	
	def pushBloodInfo(dataSet: DataSet, jobId: String, traceId: String): Unit = {
		pushMsg(BPSEvents(jobId, traceId, "SandBoxBloodJob", dataSet), isLocal = false)
	}
	
	def uploadEndPoint(uploadEnd: UploadEnd, jobId: String, traceId: String): Unit = {
		pushMsg(BPSEvents(jobId, traceId, "SandBoxBloodJob", uploadEnd), isLocal = false)
	}
}
