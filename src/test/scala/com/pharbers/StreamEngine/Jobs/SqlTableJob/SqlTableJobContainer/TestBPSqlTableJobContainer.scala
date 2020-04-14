package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer

import java.net.InetAddress

import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/10 17:34
  * @note 一些值得注意的地方
  */
class TestBPSqlTableJobContainer extends FunSuite{
    test("test BPSqlTableJobContainer exec"){
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobContainer = BPSConcertEntry.queryComponentWithId("SqlTableJobContainer").get.asInstanceOf[BPSqlTableJobContainer]
        jobContainer.exec()
        BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
                .pushMessage(write(BPSEvents("", "", "SandBox-hive", Map("datasetId" -> "", "taskType" -> "append", "url" -> "/test/testBPStream/pyJobRes/contents", "length" -> 0, "remarks" -> ""))))
        Thread.sleep(10000)
        assert(jobContainer.jobConfigs.size == 1)
//        assert(jobContainer.tasks.isEmpty)
    }
}
