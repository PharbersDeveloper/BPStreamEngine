package com.pharbers.StreamEngine.Jobs.OssPartitionJob.OssListener

import com.pharbers.StreamEngine.Utils.Channel.Driver.BPSDriverChannel
import com.pharbers.StreamEngine.Utils.Component2.BPSConcertEntry
import com.pharbers.StreamEngine.Utils.Job.BPStreamJob
import com.pharbers.StreamEngine.Utils.Event.BPSEvents
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamRemoteListener
import com.pharbers.StreamEngine.Utils.Strategy.Session.Kafka.BPKafkaSession
import com.pharbers.StreamEngine.Utils.Strategy.hdfs.BPSHDFSFile
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
    lazy val hdfsfile: BPSHDFSFile =
        BPSConcertEntry.queryComponentWithId("hdfs").get.asInstanceOf[BPSHDFSFile]

    lazy val kafka: BPKafkaSession = BPSConcertEntry.queryComponentWithId("kafka").get.asInstanceOf[BPKafkaSession]

    override def trigger(e: BPSEvents): Unit = {
        // TODO: 后面可变配置化
	    val metaDataPath = job.getMetadataPath
        val sampleDataPath = job.getOutputPath

        e.`type` match {
            case "SandBox-Schema" => {
//                BPSOssPartitionMeta.pushLineToHDFS(runId.id, event2JobId(e), e.data)
//                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
                hdfsfile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
            }
            case "SandBox-Labels" => {
//                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
                hdfsfile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
            }
            case "SandBox-Length" => {
//                BPSHDFSFile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
                hdfsfile.appendLine2HDFS(s"$metaDataPath/${event2JobId(e)}", e.data)
                //todo: 变得泛用一点
                val fileMetaData = FileMetaData(event2JobId(e), metaDataPath, sampleDataPath, "")
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

    case class FileMetaData(jobId: String, metaDataPath: String, sampleDataPath: String, convertType: String)
}
