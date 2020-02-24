package com.pharbers.StreamEngine.Utils.StreamJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import com.pharbers.util.log.PhLogable
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

trait BPStreamJob extends PhLogable{
    @transient
    type T <: BPSJobStrategy
    @transient
    val strategy: T
    @transient
    val id: String
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
}
