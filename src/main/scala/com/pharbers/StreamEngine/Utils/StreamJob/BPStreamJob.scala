package com.pharbers.StreamEngine.Utils.StreamJob

import com.pharbers.StreamEngine.Utils.StreamJob.JobStrategy.BPSJobStrategy
import com.pharbers.StreamEngine.Utils.Event.EventHandler.BPSEventHandler
import com.pharbers.StreamEngine.Utils.Event.StreamListener.BPStreamListener
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

trait BPStreamJob {
    type T <: BPSJobStrategy
    val strategy: T
    val id: String
    val spark: SparkSession
    var inputStream: Option[sql.DataFrame] = None
    var outputStream: List[StreamingQuery] = Nil
    var listeners: List[BPStreamListener] = Nil
    var handlers: List[BPSEventHandler] = Nil
    def open(): Unit = {}
    def close(): Unit = {
        //todo: log
        println("alfred clean job with id ========>" + id)
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
