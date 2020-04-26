package com.pharbers.StreamEngine.Others.alex.run

import java.net.InetAddress

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.FunSuite

class StartSandBoxJob extends FunSuite {
	test("start dcs job") {
		implicit val formats: DefaultFormats.type = DefaultFormats
		val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
		workerChannel.pushMessage(write(BPSEvents("", "", "SandBox-Start", Map())))
	}
}
