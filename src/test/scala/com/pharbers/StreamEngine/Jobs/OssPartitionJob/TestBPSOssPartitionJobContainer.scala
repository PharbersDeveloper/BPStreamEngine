package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import java.net.InetAddress

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionJobContainer.BPSOssPartitionJobContainer
import com.pharbers.StreamEngine.Utils.Channel.Worker.BPSWorkerChannel
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.ThreadExecutor.ThreadExecutor
import org.json4s._
import org.json4s.jackson.Serialization.write
import org.scalatest.FunSuite

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/08 18:07
  * @note 一些值得注意的地方
  */
class TestBPSOssPartitionJobContainer extends FunSuite{
    test("test open and exec"){
        implicit val formats: DefaultFormats.type = DefaultFormats
        BPSConcertEntry.start()
        val job = BPSConcertEntry.queryComponentWithId("OssJobContainer").get.asInstanceOf[BPSOssPartitionJobContainer]
        val workerChannel = BPSWorkerChannel(InetAddress.getLocalHost.getHostAddress)
        workerChannel.pushMessage(write(BPSEvents("", "", "SandBox-Start", Map())))
        Thread.sleep(1000 * 30)
        assert(job.spark.streams.active.length == 2)
        job.close()
        Thread.sleep(1000 * 30)
    }

}
