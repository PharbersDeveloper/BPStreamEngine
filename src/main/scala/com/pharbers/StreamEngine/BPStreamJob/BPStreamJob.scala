package com.pharbers.StreamEngine.BPStreamJob

import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.JobStrategy
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

trait BPStreamJob {
    type T <: JobStrategy
    val strategy: T
    val spark: SparkSession
    var inputStream: Option[sql.DataFrame] = None
    var outputStream: List[StreamingQuery] = Nil
    def open(): Unit
    def close(): Unit
    def exec(): Unit
}
