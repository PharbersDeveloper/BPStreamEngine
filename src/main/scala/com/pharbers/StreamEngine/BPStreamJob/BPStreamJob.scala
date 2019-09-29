package com.pharbers.StreamEngine.BPStreamJob

import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.JobStrategy
import com.pharbers.StreamEngine.Common.EventHandler.EventHandler
import com.pharbers.StreamEngine.Common.StreamListener.BPStreamListener
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

trait BPStreamJob {
    type T <: JobStrategy
    val strategy: T
    val id: String
    val spark: SparkSession
    var inputStream: Option[sql.DataFrame] = None
    var outputStream: List[StreamingQuery] = Nil
    var listeners: List[BPStreamListener] = Nil
    var handlers: List[EventHandler] = Nil
    def open(): Unit = {}
    def close(): Unit = {
        println("alfred clean job with id ========>" + id)
        handlers.foreach(_.close())
        listeners.foreach(_.deActive())
        outputStream.foreach(_.stop())
        inputStream match {
            case Some(is) =>
            case None => ???
        }
    }
    def exec(): Unit = {}
}
