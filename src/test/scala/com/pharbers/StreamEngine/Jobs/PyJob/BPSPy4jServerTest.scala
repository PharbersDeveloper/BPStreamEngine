package com.pharbers.StreamEngine.Jobs.PyJob

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
}
