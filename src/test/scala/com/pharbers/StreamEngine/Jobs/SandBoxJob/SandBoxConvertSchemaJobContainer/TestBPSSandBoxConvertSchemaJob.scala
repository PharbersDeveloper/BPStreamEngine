package com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxConvertSchemaJobContainer

import java.net.InetAddress

import com.pharbers.StreamEngine.Jobs.SandBoxJob.BPSSandBoxConvertSchemaJob
import com.pharbers.StreamEngine.Jobs.SandBoxJob.SandBoxJobContainer.BPSSandBoxJobContainer
import com.pharbers.StreamEngine.Others.alex.sandbox.FileMetaData
import com.pharbers.StreamEngine.Utils.Channel.Local.BPSLocalChannel
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import com.pharbers.util.log.PhLogable
import org.scalatest.FunSuite
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

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
    test("test open and exec"){
        implicit val formats: DefaultFormats.type = DefaultFormats
        val jobContainer = BPSConcertEntry.queryComponentWithId("SandBoxJobContainer").get.asInstanceOf[BPSSandBoxJobContainer]
        val localChanel: BPSLocalChannel = BPSConcertEntry.queryComponentWithId("local channel").get.asInstanceOf[BPSLocalChannel]
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        spark.sparkContext.setLogLevel("ERROR")
        val jobIds = spark.read.parquet("/jobs/5e95b3801d45316c2831b98b/BPSOssPartitionJob/3f84542c-f23f-4d7b-836c-c8d656e287fa/contents")
                        .select("jobId").distinct().collect().map(x => x.getAs[String]("jobId"))
        jobContainer.open()
        jobContainer.exec()
        val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
        jobIds.foreach(jobId => {
            val data = FileMetaData(jobId, "/jobs/5e95b3801d45316c2831b98b/BPSOssPartitionJob/3f84542c-f23f-4d7b-836c-c8d656e287fa/metadata",
                "/jobs/5e95b3801d45316c2831b98b/BPSOssPartitionJob/3f84542c-f23f-4d7b-836c-c8d656e287fa/contents", "")
            workerChannel.pushMessage(write(BPSEvents(jobId, "test", "SandBox-FileMetaData", data)))
            logger.info(s"jobId: $jobId")
        })
        logger.info("******************************************************")
        while (true){
            jobContainer.jobs.values.foreach(x => {
                if(x.outputStream.nonEmpty){
                    val length = try{
                        x.outputStream.head.recentProgress.map(_.numInputRows).sum
                    } catch {
                        case _: Throwable => -1
                    }
                    logger.info(s"未关闭job ${x.id} => $length query => ${x.outputStream.head.id.toString}")
                }
            })
            logger.info(s"query: ${jobContainer.jobs.size}, listeners: ${localChanel.lst.size}, events: ${localChanel.events.size()}")
            Thread.sleep(10000)
            logger.info("******************************************************")
        }
        ThreadExecutor.waitForShutdown()
    }
}
