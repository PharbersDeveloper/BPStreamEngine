package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID
import org.scalatest.FunSuite
import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jServer

class BPSPy4jServerTest extends FunSuite {
    test("test push and pop") {
        val server = BPSPy4jServer()
        assert(server.py4j_pop() == "EMPTY")
        assert(BPSPy4jServer.push("abc-1") == ())
        assert(BPSPy4jServer.push("abc-2") == ())
        assert(server.py4j_pop() == "abc-1")
        assert(server.py4j_pop() == "abc-2")
    }

    test("test startServer and startEndpoint") {

        val jobId = "abc001"
        val threadId1 = UUID.randomUUID().toString
        val threadId2 = UUID.randomUUID().toString

        val fileSuffix = "csv"
        val partitionId = 0

        val resultPath = "./jobs/" + jobId
        val rowRecordPath = resultPath + "/row_record"
        val metadataPath = resultPath + "/metadata"
        val successPath = resultPath + "/file"
        val errPath = resultPath + "/err"

        val genPath: (String, String) => String =
            (path, threadId) => s"$path/part-$partitionId-$threadId.$fileSuffix"

        BPSPy4jServer.open(Map(
            "jobId" -> jobId,
            "threadId" -> threadId1,
            "rowRecordPath" -> genPath(rowRecordPath, threadId1),
            "successPath" -> genPath(successPath, threadId1),
            "errPath" -> genPath(errPath, threadId1),
            "metadataPath" -> genPath(metadataPath, threadId1)
        ))
        BPSPy4jServer.open(Map(
            "jobId" -> jobId,
            "threadId" -> threadId2,
            "rowRecordPath" -> genPath(rowRecordPath, threadId2),
            "successPath" -> genPath(successPath, threadId2),
            "errPath" -> genPath(errPath, threadId2),
            "metadataPath" -> genPath(metadataPath, threadId2)
        ))

        assert(BPSPy4jServer.servers != Map.empty)

        for (_ <- 1 to 100) {
            BPSPy4jServer.push("abc")
            BPSPy4jServer.push("123")
            BPSPy4jServer.push("张飒")
            BPSPy4jServer.push("张飒仨")
            BPSPy4jServer.push("万宝路")
            BPSPy4jServer.push("福狼藉")
        }
        BPSPy4jServer.push("EOF")
        BPSPy4jServer.push("EOF")

        assert(BPSPy4jServer.dataQueue != Nil)

        for (_ <- 1 to 10) {
            Thread.sleep(1000)
        }

        assert(BPSPy4jServer.dataQueue == Nil)
        assert(BPSPy4jServer.servers == Map.empty)
    }
}
