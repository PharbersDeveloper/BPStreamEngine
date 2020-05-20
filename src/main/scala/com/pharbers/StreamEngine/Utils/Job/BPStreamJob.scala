package com.pharbers.StreamEngine.Utils.Job

import com.pharbers.StreamEngine.Utils.Component2.{BPComponent, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Log.PhLogable
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

trait BPStreamJob extends PhLogable with BPComponent {
    @deprecated
    @transient
    type T <: BPComponent
    @deprecated
    @transient
    val strategy: T
    @transient
    //id是BPStreamJob实例唯一标识
    val id: String
    //todo：这儿感觉需要有个jobId
    @transient
    val description: String
    @deprecated
    @transient
    val spark: SparkSession
    @transient
    var inputStream: Option[sql.DataFrame] = None
    @transient
    var outputStream: List[StreamingQuery] = Nil
    @transient
    protected var listeners: List[BPStreamListener] = Nil
    @transient
    protected var handlers: List[BPSEventHandler] = Nil
    def open(): Unit = {}
    def close(): Unit = {
        logger.info("alfred clean job with id ========>" + id)
        handlers.foreach(_.close())
        listeners.foreach(_.deActive())
        outputStream.foreach(_.stop())
        inputStream match {
            case Some(is) =>
            case None =>
        }
    }
    def exec(): Unit = {}

    // TODO: 这里应该是一个output的strategy, 为了快速重构，偷懒
    //不同job的这儿生成的目录会不同，所以目录还是需要消息传输
    def getCheckpointPath: String =
        "/jobs/" + s"runId_${BPSConcertEntry.runner_id}" + "/" + description + "/" + s"jobId_$id" + "/checkpoint"
    def getMetadataPath: String =
        "s3a://ph-stream/jobs/" + s"runId_${BPSConcertEntry.runner_id}" + "/" + description + "/" + s"jobId_$id" + "/metadata"
    def getOutputPath: String =
        "s3a://ph-stream/jobs/" + s"runId_${BPSConcertEntry.runner_id}" + "/" + description + "/" + s"jobId_$id" + "/contents"
}
