package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.net.InetAddress

import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer
import com.pharbers.StreamEngine.Others.alex.sandbox.FileMetaData
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.scalatest.FunSuite
import org.json4s.DefaultFormats

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/01/15 10:42
  * @note 一些值得注意的地方
  */
class TestBPSSandBoxConvertSchemaJob extends FunSuite with PhLogable{
    test("100rows file driver memory stress testing"){
        //1235 * 100 file
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobContainer = BPSConcertEntry.queryComponentWithId("SandBoxJobContainer").get.asInstanceOf[BPSSandBoxJobContainer]
        val localChanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel]
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        spark.sparkContext.setLogLevel("INFO")
        val jobIds = spark.read.parquet("/test/testBPStream/ossJobRes/contents")
                        .select("jobId").distinct().collect().map(x => x.getAs[String]("jobId"))
        jobContainer.open()
        jobContainer.exec()
        val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
        var step = 100
        while (step > 0){
            jobIds.foreach(jobId => {
                val data = FileMetaData(jobId, "/test/testBPStream/ossJobRes/metadata",
                    "/test/testBPStream/ossJobRes/contents", "")
                jobContainer.starJob(BPSTypeEvents(BPSEvents(jobId, "test", "SandBox-FileMetaData", data)))
                logger.info(s"jobId: $jobId")
                Thread.sleep(1000 * 3)
            })
            logger.info("******************************************************")
            step -= 1
        }

//        while (true){
////            jobContainer.jobs.values.foreach(x => {
////                if(x.outputStream.nonEmpty){
////                    val length = try{
////                        x.outputStream.head.recentProgress.map(_.numInputRows).sum
////                    } catch {
////                        case _: Throwable => -1
////                    }
////                    logger.info(s"未关闭job ${x.id} => $length query => ${x.outputStream.head.id.toString}")
////                }
////            })
//            logger.info(s"query: ${jobContainer.jobs.size}, listeners: ${localChanel.lst.size}, events: ${localChanel.events.size()}")
//            Thread.sleep(10000)
//            logger.info("******************************************************")
//        }
//        ThreadExecutor.waitForShutdown()
    }

    test("7000000rows file test"){
        val jobContainer = BPSConcertEntry.queryComponentWithId("SandBoxJobContainer").get.asInstanceOf[BPSSandBoxJobContainer]
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        spark.sparkContext.setLogLevel("INFO")
        val jobId = "test"
        val data = FileMetaData(jobId, "/test/testBPStream/ossJobRes/metadata",
            "/test/testBPStream/ossJobRes/400wContents", "")
        jobContainer.starJob(BPSTypeEvents(BPSEvents(jobId, "test", "SandBox-FileMetaData", data)))
        logger.info(s"jobId: $jobId")
        Thread.sleep(1000 * 60)
    }
}
