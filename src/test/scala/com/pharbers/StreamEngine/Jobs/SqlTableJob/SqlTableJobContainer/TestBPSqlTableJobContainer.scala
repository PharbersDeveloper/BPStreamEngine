package com.pharbers.StreamEngine.Jobs.SqlTableJob.SqlTableJobContainer

import java.net.InetAddress

import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobLocalListener
import com.pharbers.StreamEngine.Utils.Job.Status.BPSJobStatus
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/10 17:34
  * @note 一些值得注意的地方
  */
class TestBPSqlTableJobContainer extends FunSuite with BeforeAndAfterAll{
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

    test("test BPSqlTableJobContainer run job"){
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobContainer = BPSConcertEntry.queryComponentWithId("SqlTableJobContainer").get.asInstanceOf[BPSqlTableJobContainer]
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        jobContainer.exec()
        BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
                .pushMessage(write(BPSEvents("", "", "SandBox-hive", Map("datasetId" -> "", "taskType" -> "append", "url" -> "/test/testBPStream/pyJobRes/contents", "length" -> 0, "remarks" -> ""))))
        BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
                .pushMessage(write(BPSEvents("", "", "SandBox-hive", Map("datasetId" -> "", "taskType" -> "end", "url" -> "/test/testBPStream/pyJobRes/contents", "length" -> 0, "remarks" -> ""))))
        //        assert(jobContainer.tasks.isEmpty)
        val listener = BPJobLocalListener[String](null, List("job-status"))(x => {
            assert(x.data == BPSJobStatus.Success.toString)
            spark.sql("drop table test")
        })
        listener.active(null)
        Thread.sleep(30000)
        assert(jobContainer.jobs.isEmpty)
        spark.close()
    }
}
