package com.pharbers.StreamEngine.Jobs.GenCube

import java.net.InetAddress

import com.pharbers.StreamEngine.Jobs.GenCubeJob.BPSGenCubeJob
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.scalatest.FunSuite

class NewStartGenCube extends FunSuite {
	test("start gen cube job") {
		implicit val formats: DefaultFormats.type = DefaultFormats
		val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)

		val inputDataType = BPSGenCubeJob.HIVE_DATA_TYPE
		val inputPath = "SELECT * FROM result"
		val outputDataType = "es"
		val outputPath = "newcube"
		val strategy = BPSGenCubeJob.STRATEGY_CMD_HANDLE_HIVE_RESULT

		workerChannel.pushMessage(write(BPSEvents("", "", "GenCube-Start", Map(
			BPSGenCubeJob.INPUT_DATA_TYPE_KEY -> inputDataType,
			BPSGenCubeJob.INPUT_PATH_KEY -> inputPath,
			BPSGenCubeJob.OUTPUT_DATA_TYPE_KEY -> outputDataType,
			BPSGenCubeJob.OUTPUT_PATH_KEY -> outputPath,
			BPSGenCubeJob.STRATEGY_CMD_KEY -> strategy
		))))
	}
}
