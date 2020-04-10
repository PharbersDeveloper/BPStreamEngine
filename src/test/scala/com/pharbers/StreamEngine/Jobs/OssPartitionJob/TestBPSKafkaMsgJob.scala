package com.pharbers.StreamEngine.Jobs.OssPartitionJob

import java.util.UUID

import com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssPartitionJobContainer.BPSOssPartitionJobContainer
import com.pharbers.StreamEngine.Utils.Component2.{BPSComponentConfig, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPJobRemoteListener
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Strategy.Session.Spark.BPSparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2020/04/09 10:43
  * @note 一些值得注意的地方
  */
class TestBPSKafkaMsgJob extends FunSuite with BeforeAndAfterAll{
    test("test job get kafka msg"){
        val jobContainer = BPSConcertEntry.queryComponentWithId("OssJobContainer").get.asInstanceOf[BPSOssPartitionJobContainer]
        jobContainer.open()
        val msgJob = new BPSKafkaMsgJob(jobContainer, BPSComponentConfig(UUID.randomUUID().toString, "BPSOssPartitionJob", Nil, Map()))
        msgJob.open()
        msgJob.exec()
        assert(msgJob.spark.streams.active.contains(msgJob.outputStream.head))
        var getMsg = false
        val listen = BPJobRemoteListener[Map[String, String]](msgJob, List("test"))(x => {
            println(s"get msg ${x.toString}")
            getMsg = true
            assert(x.date.toString() == Map("key" -> "value").toString())
        })
        listen.active(null)
        val kafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]
        Thread.sleep(1000 * 10)
        kafkaSession.callKafka(BPSEvents("", "", "test", Map("key" -> "value")))
        Thread.sleep(1000 * 10)
        assert(getMsg)
        msgJob.close()
        jobContainer.close()
    }

    override protected def afterAll(): Unit = {
        val spark = BPSConcertEntry.queryComponentWithId("spark").get.asInstanceOf[BPSparkSession]
        spark.close()
    }
}
