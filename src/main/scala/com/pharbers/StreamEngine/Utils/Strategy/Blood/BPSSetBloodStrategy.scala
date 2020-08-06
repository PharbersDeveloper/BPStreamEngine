package com.pharbers.StreamEngine.Utils.Strategy.Blood

import java.util.UUID

import com.amazonaws.services.sqs.model.{MessageAttributeValue, SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.apache.kafka.common.config.ConfigDef

import collection.JavaConverters._

class BPSSetBloodStrategy (config: Map[String, String], @transient inoutConfigDef: ConfigDef = new ConfigDef()) {
	
//	private val getKafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]
	private val sqs: AmazonSQS = AmazonSQSClientBuilder.defaultClient()
	lazy private val url = "https://sqs.cn-northwest-1.amazonaws.com.cn/444603803904/ph-stream-schedule.fifo"
	
	def pushBloodInfo(data: AnyRef, jobId: String, traceId: String, msgTyp: String = "SandBoxDataSet"): Unit = {
		pushMessage(BPSEvents(jobId, traceId, msgTyp, data))
	}
	
	def uploadEndPoint(data: AnyRef, jobId: String, traceId: String): Unit = {
		pushMessage(BPSEvents(jobId, traceId, "UploadEndPoint", data))
	}
	
	def complementAsset(data: AnyRef, jobId: String, traceId: String): Unit = {
		pushMessage(BPSEvents(jobId, traceId, "ComplementAsset", data))
	}
	
	def setMartTags(data: AnyRef, jobId: String, traceId: String): Unit = {
		pushMessage(BPSEvents(jobId, traceId, "SetMartTags", data))
	}

	def pushMessage(events: BPSEvents): Unit ={
		val attributes = Map(
			"jobId" -> new MessageAttributeValue().withDataType("String").withStringValue(events.jobId),
			"traceId" -> new MessageAttributeValue().withDataType("String").withStringValue(events.traceId),
			"type" -> new MessageAttributeValue().withDataType("String").withStringValue(events.`type`)
		)
		val id = UUID.randomUUID().toString
		val msg = new SendMessageRequest()
				.withQueueUrl(url)
        		.withMessageBody(events.data)
        		.withMessageAttributes(attributes.asJava)
				.withMessageGroupId(id)
				.withMessageDeduplicationId(id)
		sqs.sendMessage(msg)
	}
}
