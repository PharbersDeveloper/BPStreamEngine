package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID
import org.scalatest.FunSuite
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.{BPSPy4jManager, BPSPy4jServer}

class BPSPy4jServerTest extends FunSuite {
    val runId: String = "20200323"
    val containerId: String = "BPSPy4jServerTest"

    test("test push and pop") {
        val manager: BPSPy4jManager = BPSPy4jManager()

        val jobId = UUID.randomUUID().toString
        val threadId = UUID.randomUUID().toString
        val resultPath = s"./jobs/$runId/$containerId/$jobId"

        val server = BPSPy4jServer(Map(
            "retryCount" -> "3",
            "jobId" -> jobId,
            "threadId" -> threadId,
            "rowRecordPath" -> s"$resultPath/row_record/$threadId.csv",
            "metadataPath" -> s"$resultPath/metadata/$threadId.csv",
            "successPath" -> s"$resultPath/contents/$threadId.csv",
            "errPath" -> s"$resultPath/err/$threadId.csv"
        ))(manager.pop, manager.close)

        assert(server.py4j_pop() == "EMPTY")
        assert(manager.push("abc-1") == ())
        assert(manager.push("abc-2") == ())
        assert(server.py4j_pop() == "abc-1")
        assert(server.py4j_pop() == "abc-2")
    }

    // TODO 执行测试要修改 BPSPy4jServer.startEndpoint() 中，python script 的路径
    test("test startServer and startEndpoint") {
        val manager: BPSPy4jManager = BPSPy4jManager()

        val jobId = UUID.randomUUID().toString
        val threadId1 = UUID.randomUUID().toString
        val threadId2 = UUID.randomUUID().toString
        val resultPath = s"./jobs/$runId/$containerId/$jobId"

        val fileSuffix = "csv"
        val partitionId = 0

        val rowRecordPath = resultPath + "/row_record"
        val metadataPath = resultPath + "/metadata"
        val successPath = resultPath + "/contents"
        val errPath = resultPath + "/err"

        val genPath: (String, String) => String = (path, threadId) => s"$path/part-$partitionId-$threadId.$fileSuffix"

        manager.open(Map(
            "retryCount" -> "3",
            "jobId" -> jobId,
            "threadId" -> threadId1,
            "rowRecordPath" -> genPath(rowRecordPath, threadId1),
            "metadataPath" -> genPath(metadataPath, threadId1),
            "successPath" -> genPath(successPath, threadId1),
            "errPath" -> genPath(errPath, threadId1)
        ))
        manager.open(Map(
            "retryCount" -> "3",
            "jobId" -> jobId,
            "threadId" -> threadId2,
            "rowRecordPath" -> genPath(rowRecordPath, threadId2),
            "metadataPath" -> genPath(metadataPath, threadId2),
            "successPath" -> genPath(successPath, threadId2),
            "errPath" -> genPath(errPath, threadId2)
        ))

        assert(manager.servers != Map.empty)

//        for (_ <- 1 to 100) {
//            manager.push("abc")
//            manager.push("123")
//            manager.push("张飒")
//            manager.push("张飒仨")
//            manager.push("万宝路")
//            manager.push("福狼藉")
//        }
        manager.push("EOF")
        manager.push("EOF")

        assert(manager.dataQueue != Nil)

//        for (_ <- 1 to 10) {
//            println(manager.dataQueue)
//            Thread.sleep(2000)
//        }

        assert(manager.dataQueue == Nil)
        assert(manager.servers == Map.empty)
    }
}
