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

        val hdfsAddr = "hdfs://spark.master:9000"
        val fileSuffix = "csv"
        val partitionId = 0

        val resultPath = "hdfs:///test/qi3/" + "abc001"
        val rowRecordPath = resultPath + "/row_record"
        val metadataPath = resultPath + "/metadata"
        val successPath = resultPath + "/file"
        val errPath = resultPath + "/err"

        val genPath: String => String =
            path => s"$path/part-$partitionId-${UUID.randomUUID().toString}.$fileSuffix"

        BPSPy4jServer.open(Map(
            "hdfsAddr" -> hdfsAddr,
            "rowRecordPath" -> genPath(rowRecordPath),
            "successPath" -> genPath(successPath),
            "errPath" -> genPath(errPath),
            "metadataPath" -> genPath(metadataPath)
        ))
        BPSPy4jServer.open(Map(
            "hdfsAddr" -> hdfsAddr,
            "rowRecordPath" -> genPath(rowRecordPath),
            "successPath" -> genPath(successPath),
            "errPath" -> genPath(errPath),
            "metadataPath" -> genPath(metadataPath)
        ))

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

        for (i <- 1 to 10) {
            println(BPSPy4jServer.dataQueue)
            Thread.sleep(5000)
            println(i)
        }
    }
}
