package com.pharbers.StreamEngine.Utils.Job

import com.pharbers.StreamEngine.Utils.Component2.{BPComponent, BPSConcertEntry}
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.StreamEngine.Utils.Strategy.BPStrategyComponent
import com.pharbers.util.log.PhLogable
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
    val id: String
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
    var listeners: List[BPStreamListener] = Nil
    @transient
    var handlers: List[BPSEventHandler] = Nil
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
    def getCheckpointPath: String =
        "jobs/" + BPSConcertEntry.runner_id + "/" + description + "/" + id + "/checkpoint"
    def getMetadataPath: String =
        "jobs/" + BPSConcertEntry.runner_id + "/" + description + "/" + id + "/metadata"
    def getOutputPath: String =
        "jobs/" + BPSConcertEntry.runner_id + "/" + description + "/" + id + "/contents"
}
