package com.pharbers.StreamEngine.Jobs.PyJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.PyJob.Py4jServer.BPSPy4jServer
import org.scalatest.FunSuite

class BPSPy4jServerTest extends FunSuite {
    test("test push and pop") {
        val server = BPSPy4jServer()
        assert(server.pop() == "EMPTY")
        assert(server.push("abc-1") == ())
        assert(server.push("abc-2") == ())
        assert(server.pop() == "abc-1")
        assert(server.pop() == "abc-2")
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

        val server = BPSPy4jServer(Map(
            "hdfsAddr" -> hdfsAddr,
            "rowRecordPath" -> rowRecordPath,
            "successPath" -> genPath(successPath),
            "errPath" -> genPath(errPath),
            "metadataPath" -> genPath(metadataPath)
        ))
        server.startServer()
        server.startEndpoint(server.server.getPort.toString)
        println(server.server.getPort.toString)

        Thread.sleep(10000)
    }
}
