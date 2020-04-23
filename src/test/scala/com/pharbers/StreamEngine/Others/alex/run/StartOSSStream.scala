package com.pharbers.StreamEngine.Others.alex.run

import java.net.InetAddress

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import org.scalatest.FunSuite
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

class StartOSSStream extends FunSuite {
	implicit val formats: DefaultFormats.type = DefaultFormats
	val events: BPSEvents = BPSEvents("", "", "SandBox-Start-Test", Map())
	test("start dcs local job") {
		val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
		workerChannel.pushMessage(write(events))
	}
	
	test("start dcs online job") {
		def getKafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]
		getKafka.callKafka(events)
	}
}
