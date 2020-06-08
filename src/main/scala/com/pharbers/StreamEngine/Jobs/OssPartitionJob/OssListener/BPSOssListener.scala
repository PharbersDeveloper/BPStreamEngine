package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.{BPSEvents, BPSTypeEvents}
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.Event.msgMode.FileMetaData
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Strategy.s3a.BPS3aFile
import org.apache.spark.sql.DataFrame


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/10/11 13:30
  * @note 一些值得注意的地方
  */
case class BPSOssListener(job: BPStreamJob, msgType: String) extends BPStreamRemoteListener {
    def event2JobId(e: BPSEvents): String = e.jobId
    def map2Id(map: Map[String, String]): String = map.getOrElse("id", throw new Exception("not found key of id"))
    lazy val s3aFile: BPS3aFile =
        BPSConcertEntry.queryComponentWithId("s3a").get.asInstanceOf[BPS3aFile]

    lazy val kafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]

    override def trigger(e: BPSEvents): Unit = {
	    val metaDataPath = job.getMetadataPath
        val sampleDataPath = job.getOutputPath
        val event = BPSTypeEvents[Map[String, String]](e)

        e.`type` match {
            case "SandBox-Schema" => {
                s3aFile.appendLine(s"$metaDataPath/${map2Id(event.data)}", event.data.getOrElse("data", ""))
            }
            case "SandBox-Labels" => {
                s3aFile.appendLine(s"$metaDataPath/${map2Id(event.data)}", event.data.getOrElse("data", ""))
            }
            case "SandBox-Length" => {
                s3aFile.appendLine(s"$metaDataPath/${map2Id(event.data)}", event.data.getOrElse("data", ""))
                val fileMetaData = FileMetaData(event2JobId(e), map2Id(event.data), metaDataPath, s"$sampleDataPath/jobId=${event2JobId(e)}/id=${map2Id(event.data)}", "")
                kafka.callKafka(BPSEvents(event2JobId(e), e.traceId , msgType, fileMetaData))
            }
        }
    }


    override def hit(e: BPSEvents): Boolean = e != null && (e.`type` == "SandBox-Schema" || e.`type` == "SandBox-Labels" || e.`type` == "SandBox-Length")

    override def active(s: DataFrame): Unit = {
        BPSDriverChannel.registerListener(this)
    }

    override def deActive(): Unit = {
        BPSDriverChannel.unRegisterListener(this)
    }
}
