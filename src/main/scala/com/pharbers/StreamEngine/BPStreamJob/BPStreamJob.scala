package com.pharbers.StreamEngine.BPStreamJob

import com.pharbers.StreamEngine.BPStreamJob.JobStrategy.JobStrategy
import org.apache.spark.sql.SparkSession

trait BPStreamJob[T <: JobStrategy] {
    val strategy: T
    val spark: SparkSession
    def open(): Unit
    def close(): Unit
    def exec(): Unit
}
