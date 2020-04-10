package com.pharbers.StreamEngine.Others.alex.sandbox

import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.scalatest.FunSuite

class kafkaSessionCall extends FunSuite {
	test("push sandbox job") {
		lazy val kafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka") match {
			case Some(k) => k.asInstanceOf[BPKafkaSession]
			case _ => ???
		}
		
		1 to 1000 foreach { _ =>
			val JobId = "0011ec7d-c0f1-4b57-bc18-a3a621fe46e20"
			val MetaDataPath = "/user/qianpeng/jobs/ec0cec8a-40dc-4c8c-8e00-afcd9aae8e8d/926c3114-b033-41ad-8c45-d42e6521fd1b/metadata"
			val SampleDataPath = "/user/qianpeng/jobs/ec0cec8a-40dc-4c8c-8e00-afcd9aae8e8d/926c3114-b033-41ad-8c45-d42e6521fd1b/contents"
			val fileMetaData = FileMetaData(JobId, MetaDataPath, SampleDataPath, "")
			kafka.callKafka(BPSEvents(JobId, "TraceId000999", "SandBox-FileMetaData", fileMetaData))
		}
		
	}
}

case class FileMetaData(jobId: String, metaDataPath: String, sampleDataPath: String, convertType: String)
